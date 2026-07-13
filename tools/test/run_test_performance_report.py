#!/usr/bin/env python3
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

"""Run the canonical PostgreSQL autotest case and render a timing report.

Usage from the repository root:

    python tools/test/run_test_performance_report.py

The script executes the canonical ``remote-dbs-py310-postgres14`` case through
``tests/ruciopytest``. It snapshots the checkout, builds the same AlmaLinux
runtime on the Docker server's native platform, prepares a fresh Compose
project, executes the suite, and tears the project down. The default of four
workers matches the performance-report baseline. Override it explicitly when
targeting a different environment:

    python tools/test/run_test_performance_report.py --workers 1

Python 3.9 or newer and a running Docker installation with Compose and Buildx
are required. If neither the invoking Python nor the checkout environment has
the canonical test-control dependencies, the script creates an owned private
environment under ``.autotest`` from
``requirements/requirements.devenv-control.txt``.

To run a shorter diagnostic while developing the script, pass test paths or
node IDs after ``--``:

    python tools/test/run_test_performance_report.py -- tests/test_ping.py

Every invocation creates a timestamped directory below
``.autotest/performance`` containing ``junit.xml``, ``timings.json``,
``metadata.json``, and ``report.html``. The script prints the absolute report
path when it finishes. A baseline and candidate may have different application
source fingerprints. Compare them only when both runs are marked comparable
and their comparison key and test selection match; the resolved case, harness,
actual test manifest, runtime build inputs, exact container images, worker
count, and effective Docker resources are in the key.

The source snapshot starts at the current ``HEAD`` and overlays staged and
unstaged changes plus untracked, non-ignored files. It never checks out another
branch. Ignored files are excluded so local environments, credentials, and old
reports are not copied. Tests run from a temporary Git worktree, so the real
checkout is never mounted into a container and remains unchanged.

Untracked, non-ignored files are intentionally included in the snapshot. Make
sure local credentials and other sensitive files are ignored before running
the script, as you would before running the Rucio tests directly.

Each run uses a fingerprinted runtime image and the canonical test adapter's
isolated Compose project, network, and fresh database volume. The adapter
performs normal cleanup; the report process also checks for and removes only
resources matching the exact run-owned project identity. A per-user host lock
rejects overlapping report runs because they would distort timings.

The runtime image and completed reports are intentionally retained. Normal
completion, failure, ``SIGINT``, and ``SIGTERM`` run cleanup; ``SIGKILL`` and
power loss cannot be trapped and may leave only the script's randomly named,
labelled temporary resources behind.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import re
import shlex
import shutil
import signal
import stat
import subprocess  # noqa: S404
import sys
import tempfile
import time
import xml.etree.ElementTree as ElementTree  # noqa: S405
from collections import Counter, defaultdict
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import datetime
from html import escape
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Sequence


def find_repository_root() -> Path:
    markers = (Path("tests/ruciopytest/profiles.py"), Path("etc/docker/test/runtime.Dockerfile"))
    starts = (Path(__file__).resolve().parent, Path.cwd().resolve())
    seen = set()
    for start in starts:
        for candidate in (start, *start.parents):
            if candidate in seen:
                continue
            seen.add(candidate)
            if all((candidate / marker).is_file() for marker in markers):
                return candidate
    raise SystemExit("Run this script from inside a Rucio source checkout.")


ROOT = find_repository_root()
CASE_NAME = "remote-dbs-py310-postgres14"
DEFAULT_OUTPUT_ROOT = Path(".autotest/performance")
DEFAULT_WORKERS = 4
CONTROL_REQUIREMENTS = ROOT / "requirements/requirements.devenv-control.txt"
CONTROL_VENV = ROOT / ".autotest/test-performance-control-venv"
CONTROL_VENV_MARKER = CONTROL_VENV / ".rucio-test-performance-owned"
CONTROL_VENV_MARKER_PREFIX = "rucio-test-performance-control-v1\n"
WORKSPACE_ROOT = ROOT / ".autotest/test-performance-workspaces"
RUNTIME_IMAGE_REPOSITORY = "rucio-test-performance"
CANONICAL_COMPOSE_FILES = (
    Path("etc/docker/dev/docker-compose.yml"),
    Path("etc/docker/dev/docker-compose.test.yml"),
)
CANONICAL_COMPOSE_PROFILES = ("postgres14", "test-dependencies")
JUNIT_OUTCOME_PROPERTY = "rucio.test.outcome"
BENCHMARK_HARNESS_INPUTS = (
    Path("tests"),
    Path("tools/devenv"),
    Path("tools/test/run_test_performance_report.py"),
    Path("etc/docker/dev/docker-compose.yml"),
    Path("etc/docker/dev/docker-compose.test.yml"),
    Path("etc/docker/dev/rucio/rucio_dev.cfg"),
    Path("etc/docker/test"),
    Path("requirements/requirements.devenv-control.txt"),
    Path("pyproject.toml"),
)
RUNTIME_BUILD_FILES = (
    Path("etc/docker/test/runtime.Dockerfile"),
    Path("etc/docker/test/extra/httpd.conf"),
    Path("etc/docker/test/extra/rucio.conf"),
    Path("etc/docker/test/extra/00-mpm.conf"),
    Path("etc/certs/hostcert_rucio.pem"),
    Path("etc/certs/hostcert_rucio.key.pem"),
    Path("etc/certs/rucio_ca.pem"),
    Path("etc/certs/ruciouser.pem"),
    Path("etc/certs/ruciouser.key.pem"),
    Path("etc/docker/dev/rucio/entrypoint.sh"),
)
SLOW_FRACTION = 0.05
WORKER_PATTERN = re.compile(r"created:\s+(\d+)/(\d+)\s+workers?")
IMAGE_MANIFEST_PREFIX = "RUCIO_TEST_IMAGE_MANIFEST="
REPORT_IMAGE_MANIFEST_ENV = "RUCIO_TEST_REPORT_IMAGE_MANIFEST"
REPORT_SOURCE_UID_ENV = "RUCIO_TEST_REPORT_SOURCE_UID"
REPORT_SOURCE_GID_ENV = "RUCIO_TEST_REPORT_SOURCE_GID"
TEST_CONTROL_ENVIRONMENT_VARIABLES = {
    "COMPOSE_FILE",
    "COMPOSE_PROFILES",
    "COMPOSE_PROJECT_NAME",
    "COPY_AUTOTEST_LOGS",
    "DOCKER_DEFAULT_PLATFORM",
    "DOCKER_REPO",
    "GITHUB_ACTIONS",
    "PARALLEL_AUTOTESTS",
    "PARALLEL_AUTOTESTS_FAILFAST",
    "PARALLEL_AUTOTESTS_PROCNUM",
    "PYTEST_ADDOPTS",
    "PYTEST_DISABLE_PLUGIN_AUTOLOAD",
    "PYTEST_PLUGINS",
    "PYTEST_XDIST_AUTO_NUM_WORKERS",
    "PYTHONHOME",
    "PYTHONPATH",
    "RDBMS",
    "RUCIO_DEV_PREFIX",
    "RUCIO_KEEP_TEST_DB",
    "RUCIO_MULTI_VO_LEG",
    "RUCIO_PYTEST_INNER",
    "RUCIO_SKIP_TEST_SETUP",
    "RUCIO_TAG",
    "SUITE",
    "TESTS",
    "USE_PODMAN",
}
TEST_CONTROL_ENVIRONMENT_PREFIXES = ("RUCIO_CFG_", "RUCIO_TEST_", "RUCIO_DEV_")
RUNNER_OWNED_LONG_OPTIONS = frozenset({
    "--case",
    "--case-workers",
    "--config-file",
    "--container-env",
    "--dist",
    "--dry-run",
    "--dry-run-json",
    "--junit-xml",
    "--junitxml",
    "--keep-db",
    "--list-cases",
    "--numprocesses",
    "--policy",
    "--rootdir",
    "--suite",
    "--tx",
    "--xdist-workers",
})


@dataclass(frozen=True)
class TestTiming:
    classname: str
    name: str
    duration: float
    status: str = "passed"
    status_type: str = ""
    message: str = ""
    detail: str = ""

    @property
    def module(self) -> str:
        parts = self.classname.split(".")
        return ".".join(parts[:2]) if len(parts) > 1 else self.classname

    @property
    def nodeid(self) -> str:
        return f"{self.classname}::{self.name}"

    def json_record(self) -> dict[str, object]:
        record: dict[str, object] = {
            "@classname": self.classname,
            "@name": self.name,
            "@time": f"{self.duration:.3f}",
        }
        if self.status != "passed":
            record[self.status] = {
                key: value
                for key, value in {
                    "@type": self.status_type,
                    "@message": self.message,
                    "#text": self.detail,
                }.items()
                if value
            }
        return record

    def chart_record(self) -> dict[str, object]:
        return {
            "classname": self.classname,
            "duration": self.duration,
            "module": self.module,
            "name": self.name,
            "nodeid": self.nodeid,
            "status": self.status,
        }


@dataclass(frozen=True)
class GroupSummary:
    group: str
    count: int
    duration: float
    maximum: float
    example: str


@dataclass(frozen=True)
class RunResult:
    returncode: int
    wall_time: float
    command: str
    configured_workers: int
    observed_workers: int | None
    selectors: tuple[str, ...]
    network_name: str
    compose_project: str
    cleanup_complete: bool
    removed_residual_volume_count: int
    preexisting_rucio_containers: tuple[str, ...]
    runtime_image_id: str = ""
    dependency_images: tuple[DependencyImage, ...] = ()
    runtime_probe: RuntimeProbe | None = None
    case_identity: ResolvedCase | None = None
    harness_fingerprint: str = ""
    test_manifest_fingerprint: str = ""


@dataclass(frozen=True)
class JUnitReport:
    records: tuple[TestTiming, ...]
    wall_time: float


@dataclass(frozen=True)
class DockerCleanup:
    complete: bool
    removed_residual_volume_count: int


@dataclass(frozen=True)
class DependencyImage:
    service: str
    reference: str
    image_id: str
    repo_digests: tuple[str, ...]


@dataclass(frozen=True)
class RuntimeProbe:
    system: str
    release: str
    machine: str
    python: str
    cpu_count: int
    affinity_count: int
    cpu_quota: float | None
    cpu_limit_source: str
    memory_limit_bytes: int | None
    memory_limit_source: str
    xdist_version: str


@dataclass(frozen=True)
class ResolvedCase:
    name: str
    suite: str
    group: str
    python: str
    rdbms: str
    policy: str
    test_paths: tuple[str, ...]
    exclude_paths: tuple[str, ...]
    compose_profiles: tuple[str, ...]
    environment: tuple[tuple[str, str], ...]
    xdist_enabled: bool
    runtime: str
    runtime_target: str
    runtime_platform: str | None

    def json_record(self) -> dict[str, object]:
        return {
            "name": self.name,
            "suite": self.suite,
            "group": self.group,
            "python": self.python,
            "rdbms": self.rdbms,
            "policy": self.policy or None,
            "test_paths": list(self.test_paths),
            "exclude_paths": list(self.exclude_paths),
            "compose_profiles": list(self.compose_profiles),
            "environment": dict(self.environment),
            "xdist_enabled": self.xdist_enabled,
            "runtime": self.runtime,
            "runtime_target": self.runtime_target,
            "runtime_platform": self.runtime_platform,
        }


@dataclass(frozen=True)
class RunImageManifest:
    runtime_reference: str
    runtime_image_id: str
    dependency_images: tuple[DependencyImage, ...]
    runtime_probe: RuntimeProbe


@dataclass(frozen=True)
class RuntimeEnvironment:
    host_system: str
    host_release: str
    host_machine: str
    host_cpu_count: int | None
    image: str
    image_build_fingerprint: str
    image_id: str
    image_os: str
    image_architecture: str
    runtime_system: str
    runtime_release: str
    runtime_machine: str
    runtime_python: str
    runtime_cpu_count: int | None
    runtime_affinity_count: int | None
    xdist_version: str
    dependency_images: tuple[DependencyImage, ...]
    docker_server_version: str = ""
    docker_server_os: str = ""
    docker_server_kernel: str = ""
    docker_server_architecture: str = ""
    docker_server_cpu_count: int | None = None
    docker_server_memory_bytes: int | None = None
    runtime_cpu_quota: float | None = None
    runtime_memory_limit_bytes: int | None = None
    runtime_cpu_limit_source: str = ""
    runtime_memory_limit_source: str = ""

    @property
    def uses_architecture_emulation(self) -> bool:
        host = normalized_architecture(self.docker_server_architecture or self.host_machine)
        runtime = normalized_architecture(self.runtime_machine or self.image_architecture)
        return bool(host and runtime and host != runtime)

    @property
    def dependency_fingerprint(self) -> str:
        return dependency_image_fingerprint(self.dependency_images)


@dataclass(frozen=True)
class UntrackedFile:
    path: str
    mode: int
    kind: str
    digest: str


@dataclass(frozen=True)
class SourceSnapshot:
    branch: str
    revision: str
    patch: bytes
    untracked_files: tuple[UntrackedFile, ...]
    fingerprint: str

    @property
    def dirty(self) -> bool:
        return bool(self.patch or self.untracked_files)

    @property
    def label(self) -> str:
        base = f"{self.branch or 'detached'} @ {self.revision[:12]}"
        return f"{base} + local changes ({self.fingerprint[:12]})" if self.dirty else base


@dataclass(frozen=True)
class RuntimeBuild:
    context: Path
    fingerprint: str
    image: str
    platform: str


def parse_arguments(arguments: "Sequence[str] | None" = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the Rucio remote database tests and render their timing report.",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=DEFAULT_OUTPUT_ROOT,
        help=f"Parent directory for versioned reports (default: {DEFAULT_OUTPUT_ROOT}).",
    )
    parser.add_argument(
        "--workers",
        type=positive_integer,
        default=DEFAULT_WORKERS,
        help=f"Fixed pytest-xdist worker count (default: {DEFAULT_WORKERS}).",
    )
    parser.add_argument(
        "test_selectors",
        nargs=argparse.REMAINDER,
        help="Optional test paths or node IDs, preceded by --.",
    )
    return parser.parse_args(arguments)


def positive_integer(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("worker count must be at least 1")
    return parsed


def repository_path(path: Path) -> Path:
    owned_root = ROOT / ".autotest"
    if owned_root.is_symlink():
        raise SystemExit(f"Refusing to use a symlinked report root: {owned_root}")
    resolved_root = owned_root.resolve()
    candidate = (path if path.is_absolute() else ROOT / path).resolve()
    try:
        resolved_root.relative_to(ROOT.resolve())
        candidate.relative_to(resolved_root)
    except ValueError as error:
        raise SystemExit(f"Output must be inside the owned report root {resolved_root}: {candidate}") from error
    return candidate


def ensure_private_directory(path: Path, *, parents: bool = False) -> None:
    try:
        path.mkdir(mode=0o700, parents=parents, exist_ok=True)
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(path, flags)
    except OSError as error:
        raise SystemExit(f"Could not safely create or open private directory {path}: {error}") from error
    try:
        path_stat = os.fstat(descriptor)
        if not stat.S_ISDIR(path_stat.st_mode):
            raise SystemExit(f"Refusing to use a non-directory report path: {path}")
        if hasattr(os, "getuid") and path_stat.st_uid != os.getuid():
            raise SystemExit(f"Refusing to use an unowned report directory: {path}")
        os.fchmod(descriptor, 0o700)
    finally:
        os.close(descriptor)


def open_private_regular_file(path: Path, flags: int, *, mode: int = 0o600) -> int:
    safe_flags = flags | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, safe_flags, mode)
    except OSError as error:
        raise SystemExit(f"Could not safely open private report file {path}: {error}") from error
    try:
        path_stat = os.fstat(descriptor)
        if not stat.S_ISREG(path_stat.st_mode) or path_stat.st_nlink != 1:
            raise SystemExit(f"Refusing to use a non-regular or multiply linked report file: {path}")
        if hasattr(os, "getuid") and path_stat.st_uid != os.getuid():
            raise SystemExit(f"Refusing to use an unowned report file: {path}")
        os.fchmod(descriptor, 0o600)
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


def write_private_text(path: Path, content: str) -> None:
    descriptor = open_private_regular_file(
        path,
        os.O_RDWR | os.O_CREAT | getattr(os, "O_NONBLOCK", 0),
    )
    with os.fdopen(descriptor, "w", encoding="utf-8") as output:
        os.ftruncate(output.fileno(), 0)
        output.write(content)


def read_private_text(path: Path) -> str:
    descriptor = open_private_regular_file(
        path,
        os.O_RDONLY | getattr(os, "O_NONBLOCK", 0),
    )
    with os.fdopen(descriptor, "r", encoding="utf-8") as input_file:
        return input_file.read()


def copy_private_file(source: Path, destination: Path) -> None:
    source_descriptor = open_private_regular_file(
        source,
        os.O_RDONLY | getattr(os, "O_NONBLOCK", 0),
    )
    try:
        destination_descriptor = open_private_regular_file(
            destination,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NONBLOCK", 0),
        )
    except BaseException:
        os.close(source_descriptor)
        raise
    with os.fdopen(source_descriptor, "rb") as input_file, os.fdopen(destination_descriptor, "wb") as output_file:
        shutil.copyfileobj(input_file, output_file)


def create_run_directory(output_root: Path, case: str) -> Path:
    ensure_private_directory(output_root, parents=True)
    timestamp = datetime.now().astimezone().strftime("%Y%m%d-%H%M%S")
    safe_case = "".join(character if character.isalnum() else "-" for character in case).strip("-")
    base = output_root / f"{timestamp}-{safe_case}"
    candidate = base
    suffix = 2
    while True:
        try:
            candidate.mkdir(mode=0o700)
        except FileExistsError:
            candidate = output_root / f"{base.name}-{suffix}"
            suffix += 1
            continue
        ensure_private_directory(candidate)
        return candidate


def virtualenv_python(environment: Path) -> Path:
    directory = "Scripts" if os.name == "nt" else "bin"
    executable_name = "python.exe" if os.name == "nt" else "python"
    return environment / directory / executable_name


def pinned_control_requirements(requirements: Path = CONTROL_REQUIREMENTS) -> dict[str, str]:
    pins: dict[str, str] = {}
    for line in requirements.read_text().splitlines():
        requirement = line.partition("#")[0].strip()
        if not requirement:
            continue
        name, separator, version = requirement.partition("==")
        if not separator or not name or not version:
            raise SystemExit(f"Test control requirement must be exactly pinned: {requirement}")
        pins[name] = version
    return pins


def has_control_dependencies(python: Path) -> bool:
    if not python.is_file():
        return False
    pins = pinned_control_requirements()
    probe = "import importlib.metadata, json, sys; print(json.dumps({name: importlib.metadata.version(name) for name in sys.argv[1:]}))"
    result = subprocess.run(  # noqa: S603
        [str(python), "-c", probe, *pins],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode:
        return False
    try:
        installed = json.loads(result.stdout)
    except json.JSONDecodeError:
        return False
    return installed == pins


def control_environment_marker(requirements: Path = CONTROL_REQUIREMENTS) -> str:
    return f"{CONTROL_VENV_MARKER_PREFIX}{hashlib.sha256(requirements.read_bytes()).hexdigest()}\n"


def python_executable() -> Path:
    candidates = (Path(sys.executable), virtualenv_python(ROOT / ".venv"))
    for candidate in candidates:
        if has_control_dependencies(candidate):
            return candidate

    if CONTROL_VENV.is_symlink():
        raise SystemExit(f"Refusing to use a symlinked test control environment: {CONTROL_VENV}")
    marker_content = control_environment_marker()
    if CONTROL_VENV.exists():
        if CONTROL_VENV_MARKER.is_symlink() or not CONTROL_VENV_MARKER.is_file():
            raise SystemExit(f"Refusing to replace unowned test control environment: {CONTROL_VENV}")
        current_marker = read_private_text(CONTROL_VENV_MARKER)
        if not current_marker.startswith(CONTROL_VENV_MARKER_PREFIX):
            raise SystemExit(f"Refusing to replace unowned test control environment: {CONTROL_VENV}")
        if current_marker != marker_content:
            shutil.rmtree(CONTROL_VENV)
    control_python = virtualenv_python(CONTROL_VENV)
    if not control_python.is_file():
        if CONTROL_VENV.exists() or CONTROL_VENV.is_symlink():
            raise SystemExit(f"Refusing to replace unowned test control environment: {CONTROL_VENV}")
        print(f"Creating test control environment: {CONTROL_VENV}", flush=True)
        ensure_private_directory(CONTROL_VENV.parent, parents=True)
        with tempfile.TemporaryDirectory(prefix="test-performance-control-", dir=CONTROL_VENV.parent) as temporary:
            temporary_environment = Path(temporary)
            result = run_command([sys.executable, "-m", "venv", str(temporary_environment)])
            if result.returncode:
                raise SystemExit("Could not create the test control Python environment.")
            write_private_text(temporary_environment / CONTROL_VENV_MARKER.name, marker_content)
            temporary_environment.rename(CONTROL_VENV)
    if CONTROL_VENV_MARKER.is_symlink() or not CONTROL_VENV_MARKER.is_file() or read_private_text(CONTROL_VENV_MARKER) != marker_content:
        raise SystemExit(f"Refusing to modify unowned test control environment: {CONTROL_VENV}")
    if not has_control_dependencies(control_python):
        result = run_command(
            [
                str(control_python),
                "-m",
                "pip",
                "install",
                "--disable-pip-version-check",
                "--requirement",
                str(CONTROL_REQUIREMENTS),
            ]
        )
        if result.returncode:
            raise SystemExit("Could not install the Rucio test control dependencies.")
    return control_python


def executable(name: str) -> str:
    path = shutil.which(name)
    if path is None:
        raise SystemExit(f"Required executable not found: {name}")
    return path


def stop_process_group(process: subprocess.Popen[str], description: str) -> None:
    if process.poll() is not None:
        return
    print(f"Stopping {description}...", file=sys.stderr, flush=True)
    for signum, timeout in ((signal.SIGINT, 15), (signal.SIGTERM, 10)):
        try:
            os.killpg(process.pid, signum)
            process.wait(timeout=timeout)
            return
        except ProcessLookupError:
            return
        except subprocess.TimeoutExpired:
            pass
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        return
    process.wait()


def run_command(command: "Sequence[str]", *, cwd: Path = ROOT) -> subprocess.CompletedProcess[str]:
    print("+", shlex.join(command), flush=True)
    process = subprocess.Popen(  # noqa: S603
        command,
        cwd=cwd,
        text=True,
        start_new_session=True,
    )
    try:
        returncode = process.wait()
    except BaseException:
        stop_process_group(process, shlex.join(command))
        raise
    return subprocess.CompletedProcess(command, returncode)


def git_result(
    arguments: "Sequence[str]",
    *,
    cwd: Path = ROOT,
    input_data: bytes | None = None,
) -> subprocess.CompletedProcess[bytes]:
    git = executable("git")
    return subprocess.run(  # noqa: S603
        [git, *arguments],
        cwd=cwd,
        check=False,
        input=input_data,
        capture_output=True,
    )


def require_git_result(result: subprocess.CompletedProcess[bytes], action: str) -> bytes:
    if result.returncode:
        detail = os.fsdecode(result.stderr).strip() or os.fsdecode(result.stdout).strip()
        raise SystemExit(f"Could not {action}. {detail}".rstrip())
    return result.stdout


def file_digest(path: Path) -> tuple[str, str]:
    if path.is_symlink():
        payload = os.fsencode(os.readlink(path))
        return "symlink", hashlib.sha256(payload).hexdigest()
    if not path.is_file():
        raise SystemExit(f"Cannot snapshot non-regular untracked path: {path}")
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return "file", digest.hexdigest()


def resolved_case_identity(workspace: Path, control_python: Path) -> ResolvedCase:
    probe = """
import json
import sys
from tests.ruciopytest.profiles import get_case

case = get_case(sys.argv[1])
print(json.dumps({
    "name": case.id,
    "suite": case.suite,
    "group": case.group,
    "python": case.python,
    "rdbms": case.rdbms,
    "policy": case.policy,
    "test_paths": case.test_paths,
    "exclude_paths": case.exclude_paths,
    "compose_profiles": case.compose_profiles,
    "environment": case.env_vars,
    "xdist_enabled": case.xdist_enabled,
    "runtime": case.runtime,
    "runtime_target": case.runtime_target,
    "runtime_platform": case.runtime_platform,
}, sort_keys=True))
"""
    environment = dict(os.environ)
    environment.pop("PYTHONHOME", None)
    environment.pop("PYTHONPATH", None)
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    result = subprocess.run(  # noqa: S603
        [str(control_python), "-c", probe, CASE_NAME],
        cwd=workspace,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode:
        detail = result.stderr.strip() or result.stdout.strip()
        raise SystemExit(f"Could not resolve canonical test case {CASE_NAME}. {detail}".rstrip())
    payload = load_json_object(result.stdout)

    def string_tuple(field: str) -> tuple[str, ...]:
        value = payload.get(field)
        if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
            raise SystemExit(f"Canonical test case has an invalid {field} definition.")
        return tuple(value)

    strings = {
        field: payload.get(field)
        for field in ("name", "suite", "group", "python", "rdbms", "policy", "runtime", "runtime_target")
    }
    case_environment = payload.get("environment")
    runtime_platform = payload.get("runtime_platform")
    if (
        any(not isinstance(value, str) for value in strings.values())
        or not isinstance(case_environment, dict)
        or any(not isinstance(key, str) or not isinstance(value, str) for key, value in case_environment.items())
        or not isinstance(payload.get("xdist_enabled"), bool)
        or (runtime_platform is not None and not isinstance(runtime_platform, str))
    ):
        raise SystemExit("Canonical test case returned an invalid definition.")
    identity = ResolvedCase(
        name=strings["name"],
        suite=strings["suite"],
        group=strings["group"],
        python=strings["python"],
        rdbms=strings["rdbms"],
        policy=strings["policy"],
        test_paths=string_tuple("test_paths"),
        exclude_paths=string_tuple("exclude_paths"),
        compose_profiles=string_tuple("compose_profiles"),
        environment=tuple(sorted(case_environment.items())),
        xdist_enabled=payload["xdist_enabled"],
        runtime=strings["runtime"],
        runtime_target=strings["runtime_target"],
        runtime_platform=runtime_platform,
    )
    if (
        identity.name != CASE_NAME
        or identity.suite != "remote_dbs"
        or identity.group != "autotest"
        or identity.python != "3.10"
        or identity.rdbms != "postgres14"
        or identity.policy
        or identity.compose_profiles != CANONICAL_COMPOSE_PROFILES
        or dict(identity.environment).get("RDBMS") != "postgres14"
        or dict(identity.environment).get("SUITE") != "remote_dbs"
        or not identity.xdist_enabled
        or identity.runtime != "py310"
        or identity.runtime_target != "final"
        or identity.runtime_platform is not None
    ):
        raise SystemExit(f"Canonical test case {CASE_NAME} no longer matches the performance reporter assumptions.")
    return identity


def benchmark_harness_fingerprint(workspace: Path) -> str:
    entries: dict[str, Path] = {}
    for configured_path in BENCHMARK_HARNESS_INPUTS:
        path = workspace / configured_path
        if not path.exists() and not path.is_symlink():
            raise SystemExit(f"Benchmark harness input is missing: {configured_path}")
        candidates = (path, *path.rglob("*")) if path.is_dir() and not path.is_symlink() else (path,)
        for candidate in candidates:
            entries[candidate.relative_to(workspace).as_posix()] = candidate

    digest = hashlib.sha256()
    digest.update(b"rucio-test-performance-harness-v1\0")
    for relative, path in sorted(entries.items()):
        mode = stat.S_IMODE(path.lstat().st_mode)
        if path.is_symlink() or path.is_file():
            kind, content_digest = file_digest(path)
        elif path.is_dir():
            kind, content_digest = "directory", ""
        else:
            raise SystemExit(f"Unsupported benchmark harness input: {relative}")
        digest.update(f"{relative}\0{mode:o}\0{kind}\0{content_digest}\0".encode())
    return digest.hexdigest()


def test_manifest_fingerprint(records: "Sequence[TestTiming]") -> str:
    digest = hashlib.sha256()
    digest.update(b"rucio-test-performance-test-manifest-v1\0")
    for record in sorted(records, key=lambda item: (item.classname, item.name)):
        digest.update(record.classname.encode())
        digest.update(b"\0")
        digest.update(record.name.encode())
        digest.update(b"\0")
    return digest.hexdigest()


def untracked_file(relative_name: str, *, root: Path = ROOT) -> UntrackedFile:
    relative = Path(relative_name)
    if relative.is_absolute() or ".." in relative.parts:
        raise SystemExit(f"Git returned an unsafe untracked path: {relative_name}")
    path = root / relative
    kind, digest = file_digest(path)
    return UntrackedFile(
        path=relative_name,
        mode=stat.S_IMODE(path.lstat().st_mode),
        kind=kind,
        digest=digest,
    )


def source_fingerprint(
    revision: str,
    patch: bytes,
    untracked_files: "Sequence[UntrackedFile]",
) -> str:
    digest = hashlib.sha256()
    digest.update(b"rucio-test-performance-source-v1\0")
    digest.update(revision.encode())
    digest.update(b"\0")
    digest.update(patch)
    for record in sorted(untracked_files, key=lambda item: item.path):
        digest.update(b"\0")
        digest.update(os.fsencode(record.path))
        digest.update(f"\0{record.mode:o}\0{record.kind}\0{record.digest}".encode())
    return digest.hexdigest()


def capture_source_snapshot(root: Path = ROOT) -> SourceSnapshot:
    unmerged = require_git_result(
        git_result(["diff", "--name-only", "--diff-filter=U", "-z"], cwd=root),
        "inspect the checkout for unresolved conflicts",
    )
    if unmerged:
        paths = ", ".join(os.fsdecode(path) for path in unmerged.rstrip(b"\0").split(b"\0"))
        raise SystemExit(f"Cannot snapshot a checkout with unresolved conflicts: {paths}")

    revision = os.fsdecode(require_git_result(git_result(["rev-parse", "HEAD"], cwd=root), "read HEAD")).strip()
    branch = os.fsdecode(require_git_result(git_result(["branch", "--show-current"], cwd=root), "read the current branch")).strip()
    patch = require_git_result(
        git_result(["diff", "--binary", "--no-ext-diff", "HEAD", "--"], cwd=root),
        "capture tracked checkout changes",
    )
    untracked_output = require_git_result(
        git_result(["ls-files", "--others", "--exclude-standard", "-z"], cwd=root),
        "list untracked checkout files",
    )
    names = [os.fsdecode(name) for name in untracked_output.rstrip(b"\0").split(b"\0") if name]
    files = tuple(untracked_file(name, root=root) for name in names)
    return SourceSnapshot(
        branch=branch,
        revision=revision,
        patch=patch,
        untracked_files=files,
        fingerprint=source_fingerprint(revision, patch, files),
    )


def copy_untracked_file(record: UntrackedFile, workspace: Path, source_root: Path = ROOT) -> None:
    source = source_root / record.path
    destination = workspace / record.path
    destination.parent.mkdir(parents=True, exist_ok=True)
    if record.kind == "symlink":
        os.symlink(os.readlink(source), destination)
    else:
        shutil.copy2(source, destination, follow_symlinks=False)
    copied_kind, copied_digest = file_digest(destination)
    copied_mode = stat.S_IMODE(destination.lstat().st_mode)
    if (copied_kind, copied_digest, copied_mode) != (record.kind, record.digest, record.mode):
        raise SystemExit(f"Source changed while copying untracked file: {record.path}")


@contextmanager
def isolated_source_workspace(
    snapshot: SourceSnapshot,
    source_root: Path = ROOT,
    workspace_root: Path = WORKSPACE_ROOT,
) -> "Iterator[Path]":
    ensure_private_directory(workspace_root, parents=True)
    workspace = workspace_root / f"run-{os.urandom(8).hex()}"
    marker_content = f"{source_root}\n{snapshot.fingerprint}\n"
    marker = workspace / ".rucio-test-performance-workspace"
    created = False
    try:
        result = git_result(
            ["worktree", "add", "--detach", str(workspace), snapshot.revision],
            cwd=source_root,
        )
        require_git_result(result, "create the isolated source worktree")
        created = True
        ensure_private_directory(workspace)
        write_private_text(marker, marker_content)

        if snapshot.patch:
            apply_result = git_result(
                ["apply", "--binary", "--whitespace=nowarn", "-"],
                cwd=workspace,
                input_data=snapshot.patch,
            )
            require_git_result(apply_result, "apply tracked changes to the isolated worktree")
        for record in snapshot.untracked_files:
            copy_untracked_file(record, workspace, source_root)

        if capture_source_snapshot(source_root).fingerprint != snapshot.fingerprint:
            raise SystemExit("The source checkout changed while it was being snapshotted; rerun the command.")
        yield workspace
    finally:
        if created:
            try:
                marker_is_valid = marker.is_file() and read_private_text(marker) == marker_content
            except SystemExit:
                marker_is_valid = False
            if not marker_is_valid:
                print(
                    f"Refusing to remove unverified temporary worktree: {workspace}",
                    file=sys.stderr,
                )
            else:
                cleanup = git_result(
                    ["worktree", "remove", "--force", str(workspace)],
                    cwd=source_root,
                )
                if cleanup.returncode:
                    detail = os.fsdecode(cleanup.stderr).strip() or os.fsdecode(cleanup.stdout).strip()
                    print(f"Could not remove temporary worktree {workspace}: {detail}", file=sys.stderr)


def verify_prerequisites() -> None:
    if os.name == "nt":
        raise SystemExit("Native Windows is not supported; run this script from a Rucio checkout in WSL2.")

    docker = executable("docker")
    git = executable("git")
    checks = (
        ([docker, "info"], "Docker daemon"),
        ([docker, "compose", "version"], "Docker Compose"),
        ([git, "rev-parse", "--show-toplevel"], "Git checkout"),
    )
    for command, name in checks:
        result = subprocess.run(  # noqa: S603
            command,
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode:
            detail = result.stderr.strip() or result.stdout.strip()
            raise SystemExit(f"{name} is unavailable. {detail}".rstrip())


@contextmanager
def exclusive_run_lock() -> "Iterator[None]":
    import fcntl

    user_id = os.getuid() if hasattr(os, "getuid") else 0
    lock_path = Path(tempfile.gettempdir()) / f"rucio-test-performance-report-{user_id}.lock"
    flags = os.O_CREAT | os.O_RDWR
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(lock_path, flags, 0o600)
    except OSError as error:
        raise SystemExit(f"Could not safely open the benchmark lock {lock_path}: {error}") from error
    with os.fdopen(descriptor, "r+") as lock_file:
        lock_stat = os.fstat(lock_file.fileno())
        if (
            not stat.S_ISREG(lock_stat.st_mode)
            or lock_stat.st_nlink != 1
            or (hasattr(os, "getuid") and lock_stat.st_uid != os.getuid())
        ):
            raise SystemExit(f"Refusing to use an unowned, non-regular, or multiply linked benchmark lock: {lock_path}")
        os.fchmod(lock_file.fileno(), 0o600)
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            lock_file.seek(0)
            owner = lock_file.read().strip() or "unknown"
            raise SystemExit(f"Another test performance report is running for this user (PID {owner}). Concurrent benchmark runs are not allowed.") from error
        lock_file.seek(0)
        lock_file.truncate()
        lock_file.write(f"{os.getpid()}\n")
        lock_file.flush()
        yield


@contextmanager
def termination_handler() -> "Iterator[None]":
    handled_signals = termination_signals()
    previous = {signum: signal.getsignal(signum) for signum in handled_signals}

    def interrupt(signum: int, _frame: object) -> None:
        raise KeyboardInterrupt(f"received signal {signum}")

    try:
        for signum in handled_signals:
            signal.signal(signum, interrupt)
        yield
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)


def termination_signals() -> tuple[int, ...]:
    values = [signal.SIGINT, signal.SIGTERM]
    if hasattr(signal, "SIGHUP"):
        values.append(signal.SIGHUP)
    return tuple(values)


@contextmanager
def deferred_termination_signals() -> "Iterator[None]":
    handled_signals = termination_signals()
    previous = {signum: signal.getsignal(signum) for signum in handled_signals}
    deferred = []

    def defer(signum: int, _frame: object) -> None:
        deferred.append(signum)

    try:
        for signum in handled_signals:
            signal.signal(signum, defer)
        yield
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)
    if deferred:
        raise KeyboardInterrupt(f"received signal {deferred[0]} during cleanup")


def running_rucio_containers() -> tuple[str, ...]:
    docker = executable("docker")
    result = subprocess.run(  # noqa: S603
        [
            docker,
            "ps",
            "--filter",
            "label=com.docker.compose.service=rucio",
            "--format",
            "{{.Names}}",
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    return tuple(sorted(name for name in result.stdout.splitlines() if name))


def validate_runtime_platform(value: str) -> str:
    runtime_platform = value.strip().lower()
    if re.fullmatch(r"[a-z0-9._-]+/[a-z0-9._-]+", runtime_platform) is None:
        raise SystemExit(f"Docker returned an invalid server platform: {value!r}")
    return runtime_platform


def docker_server_platform(workspace: Path = ROOT) -> str:
    docker = executable("docker")
    result = subprocess.run(  # noqa: S603
        [docker, "version", "--format", "{{.Server.Os}}/{{.Server.Arch}}"],
        cwd=workspace,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode:
        detail = result.stderr.strip() or result.stdout.strip()
        raise SystemExit(f"Docker server platform is unavailable. {detail}".rstrip())
    return validate_runtime_platform(result.stdout)


def copy_runtime_build_inputs(workspace: Path) -> Path:
    context = workspace / ".autotest/test-performance-build-context"
    ensure_private_directory(context, parents=True)
    for relative in RUNTIME_BUILD_FILES:
        source = workspace / relative
        if not source.is_file() and not source.is_symlink():
            raise SystemExit(f"Runtime build input is missing: {relative}")
        destination = context / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        if source.is_symlink():
            os.symlink(os.readlink(source), destination)
        else:
            shutil.copy2(source, destination)
    shutil.copytree(workspace / "requirements", context / "requirements", symlinks=True)
    return context


def runtime_build_fingerprint(context: Path, runtime_platform: str) -> str:
    digest = hashlib.sha256()
    digest.update(f"rucio-test-performance-runtime-v2\0{runtime_platform}\0python=3.10\0target=final".encode())
    for path in sorted(context.rglob("*"), key=lambda item: item.as_posix()):
        relative = path.relative_to(context).as_posix()
        mode = stat.S_IMODE(path.lstat().st_mode)
        if path.is_symlink() or path.is_file():
            kind, content_digest = file_digest(path)
        elif path.is_dir():
            kind, content_digest = "directory", ""
        else:
            raise SystemExit(f"Unsupported runtime build input: {relative}")
        digest.update(f"\0{relative}\0{mode:o}\0{kind}\0{content_digest}".encode())
    return digest.hexdigest()


def prepare_runtime_build(workspace: Path, runtime_platform: str) -> RuntimeBuild:
    context = copy_runtime_build_inputs(workspace)
    fingerprint = runtime_build_fingerprint(context, runtime_platform)
    architecture = runtime_platform.split("/", 1)[1]
    return RuntimeBuild(
        context=context,
        fingerprint=fingerprint,
        image=f"{RUNTIME_IMAGE_REPOSITORY}:py310-{architecture}-{fingerprint[:16]}",
        platform=runtime_platform,
    )


def runtime_build_command(docker: str, build: RuntimeBuild) -> list[str]:
    return [
        docker,
        "buildx",
        "build",
        "--load",
        "--progress=plain",
        f"--platform={build.platform}",
        "--file=etc/docker/test/runtime.Dockerfile",
        "--target=final",
        "--build-arg=PYTHON=3.10",
        "--label=org.rucio.test-performance=true",
        f"--label=org.rucio.test-performance.build-fingerprint={build.fingerprint}",
        f"--tag={build.image}",
        ".",
    ]


def build_runtime_image(build: RuntimeBuild) -> int:
    docker = executable("docker")
    inspect = subprocess.run(  # noqa: S603
        [
            docker,
            "image",
            "inspect",
            "--format",
            '{{ index .Config.Labels "org.rucio.test-performance.build-fingerprint" }}',
            build.image,
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if inspect.returncode == 0:
        if inspect.stdout.strip() != build.fingerprint:
            raise SystemExit(f"Refusing to reuse or replace image {build.image}: its build fingerprint label does not match.")
        print(f"Using matching test runtime image: {build.image}", flush=True)
        return 0

    buildx = subprocess.run(  # noqa: S603
        [docker, "buildx", "version"],
        cwd=build.context,
        check=False,
        capture_output=True,
        text=True,
    )
    if buildx.returncode:
        detail = buildx.stderr.strip() or buildx.stdout.strip()
        raise SystemExit(f"Docker Buildx is unavailable. {detail}".rstrip())

    return run_command(runtime_build_command(docker, build), cwd=build.context).returncode


def normalized_architecture(value: str) -> str:
    return {
        "aarch64": "arm64",
        "arm64": "arm64",
        "amd64": "amd64",
        "x86_64": "amd64",
    }.get(value.lower(), value.lower())


def load_json_object(value: str) -> dict[str, object]:
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def optional_integer(value: object) -> int | None:
    return value if isinstance(value, int) else None


def inspect_runtime_environment(
    workspace: Path,
    runtime_image: str,
    runtime_image_id: str,
    image_build_fingerprint: str,
    dependency_images: tuple[DependencyImage, ...],
    runtime_probe: RuntimeProbe,
) -> RuntimeEnvironment:
    docker = executable("docker")
    image_result = subprocess.run(  # noqa: S603
        [docker, "image", "inspect", "--format", "{{json .}}", runtime_image_id],
        cwd=workspace,
        check=False,
        capture_output=True,
        text=True,
    )
    if image_result.returncode:
        detail = image_result.stderr.strip() or image_result.stdout.strip()
        raise SystemExit(f"Could not inspect the runtime image used by the canonical run. {detail}".rstrip())
    image = load_json_object(image_result.stdout)
    if image.get("Id") != runtime_image_id:
        raise SystemExit("Docker returned an inconsistent runtime image identity.")
    labels = image.get("Config", {})
    labels = labels.get("Labels", {}) if isinstance(labels, dict) else {}
    if not isinstance(labels, dict) or labels.get("org.rucio.test-performance.build-fingerprint") != image_build_fingerprint:
        raise SystemExit("The runtime image used by the canonical run does not match its build fingerprint.")
    image_os = str(image.get("Os", ""))
    image_architecture = str(image.get("Architecture", ""))

    docker_info_result = subprocess.run(  # noqa: S603
        [docker, "info", "--format", "{{json .}}"],
        cwd=workspace,
        check=False,
        capture_output=True,
        text=True,
    )
    docker_info = load_json_object(docker_info_result.stdout)
    if docker_info_result.returncode:
        print("Warning: Docker server metadata probe failed; comparison fields will be incomplete.", file=sys.stderr)

    return RuntimeEnvironment(
        host_system=platform.system(),
        host_release=platform.release(),
        host_machine=platform.machine(),
        host_cpu_count=os.cpu_count(),
        image=runtime_image,
        image_build_fingerprint=image_build_fingerprint,
        image_id=runtime_image_id,
        image_os=image_os,
        image_architecture=image_architecture,
        runtime_system=runtime_probe.system,
        runtime_release=runtime_probe.release,
        runtime_machine=runtime_probe.machine,
        runtime_python=runtime_probe.python,
        runtime_cpu_count=runtime_probe.cpu_count,
        runtime_affinity_count=runtime_probe.affinity_count,
        xdist_version=runtime_probe.xdist_version,
        dependency_images=dependency_images,
        docker_server_version=str(docker_info.get("ServerVersion", "")),
        docker_server_os=str(docker_info.get("OperatingSystem", docker_info.get("OSType", ""))),
        docker_server_kernel=str(docker_info.get("KernelVersion", "")),
        docker_server_architecture=str(docker_info.get("Architecture", "")),
        docker_server_cpu_count=optional_integer(docker_info.get("NCPU")),
        docker_server_memory_bytes=optional_integer(docker_info.get("MemTotal")),
        runtime_cpu_quota=runtime_probe.cpu_quota,
        runtime_memory_limit_bytes=runtime_probe.memory_limit_bytes,
        runtime_cpu_limit_source=runtime_probe.cpu_limit_source,
        runtime_memory_limit_source=runtime_probe.memory_limit_source,
    )


def stop_runner(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    print("Stopping the isolated test runner before workspace cleanup...", file=sys.stderr, flush=True)
    try:
        os.killpg(process.pid, signal.SIGINT)
    except ProcessLookupError:
        return
    try:
        output, _ = process.communicate(timeout=90)
        if output:
            print(output, end="", flush=True)
        return
    except subprocess.TimeoutExpired:
        pass

    try:
        os.killpg(process.pid, signal.SIGTERM)
        output, _ = process.communicate(timeout=30)
        if output:
            print(output, end="", flush=True)
        return
    except (ProcessLookupError, subprocess.TimeoutExpired):
        pass

    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        return
    output, _ = process.communicate()
    if output:
        print(output, end="", flush=True)


def force_stop_runner(process: subprocess.Popen[str]) -> None:
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        return
    output, _ = process.communicate()
    if output:
        print(output, end="", flush=True)


def docker_resource_names(
    command: "Sequence[str]",
    project: str,
    name_format: str,
) -> tuple[tuple[str, ...], bool]:
    docker = executable("docker")
    result = subprocess.run(  # noqa: S603
        [docker, *command, "--filter", f"label=com.docker.compose.project={project}", "--format", name_format],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode:
        detail = result.stderr.strip() or result.stdout.strip()
        print(f"Warning: could not inspect run-owned Docker resources: {detail}", file=sys.stderr)
        return (), False
    return tuple(name for name in result.stdout.splitlines() if name), True


def remove_docker_resources(resource: str, names: "Sequence[str]") -> tuple[int, bool]:
    if not names:
        return 0, True
    docker = executable("docker")
    result = subprocess.run(  # noqa: S603
        [docker, resource, "rm", "--force", *names],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode:
        detail = result.stderr.strip() or result.stdout.strip()
        print(f"Warning: could not remove run-owned Docker {resource}s: {detail}", file=sys.stderr)
        return 0, False
    return len(names), True


def canonical_project_name(case: str, nonce: str) -> str:
    if re.fullmatch(r"[0-9a-f]{8}", nonce) is None:
        raise ValueError("project nonce must contain exactly eight lowercase hexadecimal characters")
    slug = "".join(character if character.isalnum() else "-" for character in case.lower()).strip("-")
    return f"rucio-test-{slug[:40]}-{nonce}"


def normalize_test_selectors(test_selectors: "Sequence[str]") -> tuple[str, ...]:
    selectors = tuple(test_selectors)
    normalized = selectors[1:] if selectors[:1] == ("--",) else selectors
    validate_test_selectors(normalized)
    return normalized


def validate_test_selectors(selectors: "Sequence[str]") -> None:
    for argument in selectors:
        if argument == "--":
            return
        option = argument.split("=", 1)[0]
        if (
            option in RUNNER_OWNED_LONG_OPTIONS
            or argument == "-d"
            or argument == "-c"
            or (argument.startswith("-c") and len(argument) > 2)
            or argument == "-n"
            or (argument.startswith("-n") and len(argument) > 2)
        ):
            raise ValueError(f"Reporter-owned pytest option is not allowed in the test selection: {argument}")


def canonical_test_command(
    workspace: Path,
    xml_path: Path,
    selectors: "Sequence[str]",
    workers: int,
    control_python: Path,
) -> list[str]:
    try:
        relative_xml = xml_path.resolve().relative_to(workspace.resolve())
    except ValueError as error:
        raise ValueError(f"JUnit output must be inside the isolated workspace: {xml_path}") from error
    return [
        str(control_python),
        "-m",
        "pytest",
        f"--case={CASE_NAME}",
        f"--xdist-workers={workers}",
        f"--junitxml=/rucio_source/{relative_xml.as_posix()}",
        "-p",
        "tests.ruciopytest.performance_outcomes",
        *selectors,
    ]


def canonical_test_environment(
    environment: "dict[str, str]",
    *,
    runtime_image: str,
    runtime_platform: str,
    nonce: str,
) -> dict[str, str]:
    values = dict(environment)
    for name in tuple(values):
        if (
            name in TEST_CONTROL_ENVIRONMENT_VARIABLES
            or name == "POLICY"
            or name.startswith(TEST_CONTROL_ENVIRONMENT_PREFIXES)
            or (name.startswith("RUCIO_") and name.endswith("_CONTAINER_NAME"))
        ):
            values.pop(name)
    values.update(
        {
            "GITHUB_ACTIONS": "true",
            REPORT_IMAGE_MANIFEST_ENV: "1",
            "RUCIO_DEV_PLATFORM": runtime_platform,
            "RUCIO_TEST_IMAGE": runtime_image,
            "RUCIO_TEST_NATIVE_PLATFORM": runtime_platform,
            "RUCIO_TEST_PROJECT_NONCE": nonce,
        }
    )
    if hasattr(os, "getuid") and hasattr(os, "getgid"):
        values[REPORT_SOURCE_UID_ENV] = str(os.getuid())
        values[REPORT_SOURCE_GID_ENV] = str(os.getgid())
    return values


def canonical_compose_environment(
    environment: "dict[str, str]",
    *,
    runtime_image: str,
    runtime_platform: str,
    nonce: str,
) -> dict[str, str]:
    values = canonical_test_environment(
        environment,
        runtime_image=runtime_image,
        runtime_platform=runtime_platform,
        nonce=nonce,
    )
    project = canonical_project_name(CASE_NAME, nonce)
    values.update(
        {
            "DEV_PROFILES": ",".join(CANONICAL_COMPOSE_PROFILES),
            "RDBMS": "postgres14",
            "RUCIO_HOME": "/opt/rucio",
            "RUCIO_NETWORK_NAME": f"{project}-network",
            "RUCIO_TEST_IMAGE_PLATFORM": runtime_platform,
            "RUCIO_TEST_IMAGE_TARGET": "final",
            "SUITE": "remote_dbs",
        }
    )
    return values


def canonical_compose_command(
    docker: str,
    workspace: Path,
    project: str,
    *arguments: str,
) -> list[str]:
    command = [docker, "compose", "-p", project]
    for compose_file in CANONICAL_COMPOSE_FILES:
        command.extend(("-f", str(workspace / compose_file)))
    for profile in CANONICAL_COMPOSE_PROFILES:
        command.extend(("--profile", profile))
    command.extend(arguments)
    return command


def dependency_image_fingerprint(images: "Sequence[DependencyImage]") -> str:
    digest = hashlib.sha256()
    digest.update(b"rucio-test-performance-dependencies-v1\0")
    for image in sorted(images, key=lambda item: (item.service, item.reference)):
        digest.update(image.service.encode())
        digest.update(b"\0")
        digest.update(image.reference.encode())
        digest.update(b"\0")
        digest.update(image.image_id.encode())
        for repo_digest in sorted(image.repo_digests):
            digest.update(b"\0")
            digest.update(repo_digest.encode())
        digest.update(b"\0\0")
    return digest.hexdigest()


def parse_run_image_manifest(line: str, expected_runtime: str) -> RunImageManifest:
    if not line.startswith(IMAGE_MANIFEST_PREFIX):
        raise ValueError("Not a canonical run image manifest")
    payload = load_json_object(line.removeprefix(IMAGE_MANIFEST_PREFIX))
    runtime = payload.get("runtime")
    runtime_environment = payload.get("runtime_environment")
    dependencies = payload.get("dependencies")
    if not isinstance(runtime, dict) or not isinstance(runtime_environment, dict) or not isinstance(dependencies, list):
        raise ValueError("Canonical run image manifest has an invalid shape")

    def image_record(value: object) -> DependencyImage:
        if not isinstance(value, dict):
            raise ValueError("Canonical run image manifest contains an invalid image record")
        service = value.get("service")
        reference = value.get("reference")
        image_id = value.get("image_id")
        repo_digests = value.get("repo_digests")
        if (
            not isinstance(service, str)
            or not service
            or not isinstance(reference, str)
            or not reference
            or not isinstance(image_id, str)
            or re.fullmatch(r"sha256:[0-9a-f]{64}", image_id) is None
            or not isinstance(repo_digests, list)
            or any(not isinstance(item, str) for item in repo_digests)
        ):
            raise ValueError("Canonical run image manifest contains an incomplete image identity")
        return DependencyImage(
            service=service,
            reference=reference,
            image_id=image_id,
            repo_digests=tuple(sorted(repo_digests)),
        )

    def runtime_probe_record(value: dict[str, object]) -> RuntimeProbe:
        strings = {
            field: value.get(field)
            for field in (
                "system",
                "release",
                "machine",
                "python",
                "cpu_limit_source",
                "memory_limit_source",
                "xdist_version",
            )
        }
        cpu_count = value.get("cpu_count")
        affinity_count = value.get("affinity_count")
        cpu_quota = value.get("cpu_quota")
        memory_limit_bytes = value.get("memory_limit_bytes")
        if (
            any(not isinstance(item, str) or not item for item in strings.values())
            or strings["cpu_limit_source"] not in {"cgroup-v1", "cgroup-v2"}
            or strings["memory_limit_source"] not in {"cgroup-v1", "cgroup-v2"}
            or not isinstance(cpu_count, int)
            or isinstance(cpu_count, bool)
            or cpu_count < 1
            or not isinstance(affinity_count, int)
            or isinstance(affinity_count, bool)
            or affinity_count < 1
            or (cpu_quota is not None and (not isinstance(cpu_quota, (int, float)) or isinstance(cpu_quota, bool) or cpu_quota <= 0))
            or (
                memory_limit_bytes is not None
                and (not isinstance(memory_limit_bytes, int) or isinstance(memory_limit_bytes, bool) or memory_limit_bytes < 1)
            )
        ):
            raise ValueError("Canonical run image manifest contains an incomplete runtime environment")
        return RuntimeProbe(
            system=strings["system"],
            release=strings["release"],
            machine=strings["machine"],
            python=strings["python"],
            cpu_count=cpu_count,
            affinity_count=affinity_count,
            cpu_quota=float(cpu_quota) if cpu_quota is not None else None,
            cpu_limit_source=strings["cpu_limit_source"],
            memory_limit_bytes=memory_limit_bytes,
            memory_limit_source=strings["memory_limit_source"],
            xdist_version=strings["xdist_version"],
        )

    runtime_record = image_record(runtime)
    runtime_probe = runtime_probe_record(runtime_environment)
    dependency_records = tuple(sorted((image_record(value) for value in dependencies), key=lambda item: item.service))
    services = [record.service for record in dependency_records]
    if runtime_record.service != "rucio" or runtime_record.reference != expected_runtime:
        raise ValueError("Canonical run image manifest does not describe the requested runtime")
    if not dependency_records or len(services) != len(set(services)):
        raise ValueError("Canonical run image manifest must contain unique external dependency services")
    return RunImageManifest(
        runtime_reference=runtime_record.reference,
        runtime_image_id=runtime_record.image_id,
        dependency_images=dependency_records,
        runtime_probe=runtime_probe,
    )


def cleanup_compose_project(project: str, network_name: str) -> DockerCleanup:
    nonce = project.rsplit("-", 1)[-1]
    try:
        expected_project = canonical_project_name(CASE_NAME, nonce)
    except ValueError:
        expected_project = ""
    if project != expected_project or network_name != f"{project}-network":
        print(f"Warning: refusing to clean an unexpected Compose project identity: {project}", file=sys.stderr)
        return DockerCleanup(complete=False, removed_residual_volume_count=0)

    complete = True
    containers, listed = docker_resource_names(("ps", "--all"), project, "{{.Names}}")
    complete &= listed
    owned_containers = tuple(name for name in containers if name.startswith(f"{project}-"))
    if len(owned_containers) != len(containers):
        print("Warning: ignored a container with an unexpected run ownership name.", file=sys.stderr)
        complete = False
    _, removed = remove_docker_resources("container", owned_containers)
    complete &= removed

    networks, listed = docker_resource_names(("network", "ls"), project, "{{.Name}}")
    complete &= listed
    owned_networks = tuple(name for name in networks if name == network_name)
    if len(owned_networks) != len(networks):
        print("Warning: ignored a network with an unexpected run ownership name.", file=sys.stderr)
        complete = False
    _, removed = remove_docker_resources("network", owned_networks)
    complete &= removed

    volumes, listed = docker_resource_names(("volume", "ls"), project, "{{.Name}}")
    complete &= listed
    owned_volumes = tuple(name for name in volumes if name.startswith(f"{project}_"))
    if len(owned_volumes) != len(volumes):
        print("Warning: ignored a volume with an unexpected run ownership name.", file=sys.stderr)
        complete = False
    removed_volume_count, removed = remove_docker_resources("volume", owned_volumes)
    complete &= removed
    if removed_volume_count:
        print(f"Removed {removed_volume_count} residual run-owned Docker volumes.", flush=True)
    return DockerCleanup(complete=complete, removed_residual_volume_count=removed_volume_count)


def run_tests(
    workspace: Path,
    xml_path: Path,
    test_selectors: "Sequence[str]",
    workers: int,
    control_python: Path,
    runtime_image: str,
    runtime_platform: str,
    preexisting_rucio_containers: tuple[str, ...],
    case_identity: ResolvedCase,
    harness_fingerprint: str,
) -> RunResult:
    selectors = normalize_test_selectors(test_selectors)
    nonce = os.urandom(4).hex()
    compose_project = canonical_project_name(CASE_NAME, nonce)
    network_name = f"{compose_project}-network"
    command = canonical_test_command(workspace, xml_path, selectors, workers, control_python)
    print("+", shlex.join(command), flush=True)
    environment = canonical_test_environment(
        dict(os.environ),
        runtime_image=runtime_image,
        runtime_platform=runtime_platform,
        nonce=nonce,
    )
    started = time.monotonic()
    observed_workers = None
    image_manifest = None
    cleanup = DockerCleanup(complete=False, removed_residual_volume_count=0)
    process = subprocess.Popen(  # noqa: S603
        command,
        cwd=workspace,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        start_new_session=True,
    )
    completed = False
    try:
        assert process.stdout is not None
        for output_line in process.stdout:
            if output_line.startswith(IMAGE_MANIFEST_PREFIX):
                if image_manifest is not None:
                    raise RuntimeError("Canonical test adapter emitted more than one image manifest")
                image_manifest = parse_run_image_manifest(output_line.strip(), runtime_image)
                print("Captured exact runtime and dependency image identities.", flush=True)
                continue
            print(output_line, end="", flush=True)
            worker_match = WORKER_PATTERN.search(output_line)
            if worker_match:
                observed_workers = int(worker_match.group(1))
        returncode = process.wait()
        completed = True
    except BaseException:
        with deferred_termination_signals():
            stop_runner(process)
        raise
    finally:
        with deferred_termination_signals():
            if not completed:
                force_stop_runner(process)
            cleanup = cleanup_compose_project(compose_project, network_name)
    return RunResult(
        returncode=returncode,
        wall_time=time.monotonic() - started,
        command=shlex.join(command),
        configured_workers=workers,
        observed_workers=observed_workers,
        selectors=selectors,
        network_name=network_name,
        compose_project=compose_project,
        cleanup_complete=cleanup.complete,
        removed_residual_volume_count=cleanup.removed_residual_volume_count,
        preexisting_rucio_containers=preexisting_rucio_containers,
        runtime_image_id=image_manifest.runtime_image_id if image_manifest else "",
        dependency_images=image_manifest.dependency_images if image_manifest else (),
        runtime_probe=image_manifest.runtime_probe if image_manifest else None,
        case_identity=case_identity,
        harness_fingerprint=harness_fingerprint,
    )


def load_junit_report(xml_path: Path) -> JUnitReport:
    # The JUnit XML is generated locally by the pytest invocation above.
    root = ElementTree.parse(xml_path).getroot()  # noqa: S314
    if root is None:
        raise ValueError(f"JUnit XML has no root element: {xml_path}")
    records = []
    for testcase in root.iter("testcase"):
        status = "passed"
        status_type = ""
        message = ""
        detail = ""
        for tag in ("skipped", "failure", "error"):
            child = testcase.find(tag)
            if child is not None:
                status_type = child.get("type", "")
                status = "xfailed" if tag == "skipped" and status_type == "pytest.xfail" else tag
                message = child.get("message", "")
                detail = child.text or ""
                break
        declared_outcomes = {
            property_node.get("value", "")
            for property_node in testcase.findall("./properties/property")
            if property_node.get("name") == JUNIT_OUTCOME_PROPERTY
        }
        if declared_outcomes - {"xpassed"} or len(declared_outcomes) > 1:
            raise ValueError(f"JUnit XML has an invalid {JUNIT_OUTCOME_PROPERTY} property")
        if declared_outcomes:
            if status != "passed":
                raise ValueError(f"JUnit XML declares xpassed for a {status} testcase")
            status = "xpassed"
        duration = float(f"{float(testcase.get('time', '0')):.3f}")
        records.append(
            TestTiming(
                classname=testcase.get("classname", ""),
                name=testcase.get("name", ""),
                duration=duration,
                status=status,
                status_type=status_type,
                message=message,
                detail=detail,
            )
        )
    wall_time = max(
        (float(testsuite.get("time", "0")) for testsuite in root.iter("testsuite")),
        default=0,
    )
    return JUnitReport(
        records=tuple(sorted(records, key=lambda record: (record.duration, record.classname, record.name))),
        wall_time=wall_time,
    )


def write_json(records: "Sequence[TestTiming]", output: Path) -> None:
    write_private_text(output, json.dumps([record.json_record() for record in records], indent=2) + "\n")


def percentile(sorted_values: "Sequence[float]", percentage: float) -> float:
    if not sorted_values:
        return 0.0
    rank = (len(sorted_values) - 1) * percentage / 100
    low = math.floor(rank)
    high = math.ceil(rank)
    if low == high:
        return sorted_values[low]
    return sorted_values[low] + (sorted_values[high] - sorted_values[low]) * (rank - low)


def format_seconds(value: float) -> str:
    if value >= 100:
        return f"{value:,.1f}s"
    if value >= 10:
        return f"{value:,.2f}s"
    return f"{value:,.3f}s"


def grouped_tail(
    records: "Sequence[TestTiming]",
    key: "Callable[[TestTiming], str]",
) -> list[GroupSummary]:
    groups: dict[str, list[TestTiming]] = defaultdict(list)
    for record in records:
        groups[key(record)].append(record)
    summaries = [
        GroupSummary(
            group=name,
            count=len(group_records),
            duration=sum(record.duration for record in group_records),
            maximum=max(record.duration for record in group_records),
            example=max(group_records, key=lambda record: record.duration).nodeid,
        )
        for name, group_records in groups.items()
    ]
    return sorted(summaries, key=lambda group: group.duration, reverse=True)


def stat_cell(label: str, value: str) -> str:
    return f'<div class="stat"><span>{escape(label)}</span><strong>{escape(value)}</strong></div>'


def group_rows(groups: "Sequence[GroupSummary]", slow_count: int, total_tail: float) -> str:
    rows = []
    for group in groups[:8]:
        rows.append(
            "<tr>"
            f"<td>{escape(group.group)}</td>"
            f"<td>{group.count}</td>"
            f"<td>{group.count / slow_count:.1%}</td>"
            f"<td>{format_seconds(group.duration)}</td>"
            f"<td>{group.duration / total_tail if total_tail else 0:.1%}</td>"
            f"<td>{format_seconds(group.maximum)}</td>"
            f"<td>{escape(group.example)}</td>"
            "</tr>"
        )
    return "\n".join(rows)


def slow_rows(records: "Sequence[TestTiming]", total: float) -> str:
    rows = []
    for rank, record in enumerate(records, start=1):
        rows.append(
            "<tr>"
            f"<td>{rank}</td>"
            f"<td>{format_seconds(record.duration)}</td>"
            f"<td>{record.duration / total if total else 0:.2%}</td>"
            f'<td class="status-{escape(record.status)}">{escape(record.status)}</td>'
            f"<td>{escape(record.module)}</td>"
            f"<td>{escape(record.classname)}</td>"
            f"<td>{escape(record.name)}</td>"
            "</tr>"
        )
    return "\n".join(rows)


def make_comment(
    module_groups: "Sequence[GroupSummary]",
    class_groups: "Sequence[GroupSummary]",
    total_tail: float,
    total: float,
) -> str:
    modules = ", ".join(f"{group.group} ({group.count} tests, {format_seconds(group.duration)})" for group in module_groups[:3])
    classes = ", ".join(f"{group.group} ({group.count} tests)" for group in class_groups[:3])
    tail_share = total_tail / total if total else 0
    return f"The slowest 5% accounts for {format_seconds(total_tail)} ({tail_share:.1%}) of summed testcase time. The largest module clusters are {modules}. By class, the densest clusters are {classes}."


def benchmark_environment_identity(environment: RuntimeEnvironment, result: RunResult) -> dict[str, object]:
    return {
        "schema": 2,
        "case": result.case_identity.json_record() if result.case_identity else None,
        "harness_fingerprint": result.harness_fingerprint,
        "test_manifest_fingerprint": result.test_manifest_fingerprint,
        "workers": result.configured_workers,
        "runtime": {
            "build_fingerprint": environment.image_build_fingerprint,
            "image_id": environment.image_id,
            "image_os": environment.image_os,
            "image_architecture": environment.image_architecture,
            "system": environment.runtime_system,
            "release": environment.runtime_release,
            "machine": environment.runtime_machine,
            "python": environment.runtime_python,
            "logical_cpu_count": environment.runtime_cpu_count,
            "affinity_cpu_count": environment.runtime_affinity_count,
            "cpu_quota": environment.runtime_cpu_quota,
            "cpu_limit_source": environment.runtime_cpu_limit_source,
            "memory_limit_bytes": environment.runtime_memory_limit_bytes,
            "memory_limit_source": environment.runtime_memory_limit_source,
            "xdist_version": environment.xdist_version,
        },
        "docker_server": {
            "version": environment.docker_server_version,
            "os": environment.docker_server_os,
            "kernel": environment.docker_server_kernel,
            "architecture": environment.docker_server_architecture,
            "logical_cpu_count": environment.docker_server_cpu_count,
            "memory_bytes": environment.docker_server_memory_bytes,
        },
        "dependency_fingerprint": environment.dependency_fingerprint,
    }


def benchmark_environment_fingerprint(environment: RuntimeEnvironment, result: RunResult) -> str:
    payload = json.dumps(
        benchmark_environment_identity(environment, result),
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def comparison_key(environment: RuntimeEnvironment, result: RunResult) -> str:
    return f"{CASE_NAME};workers={result.configured_workers};environment=sha256:{benchmark_environment_fingerprint(environment, result)}"


def comparison_invalid_reasons(environment: RuntimeEnvironment, result: RunResult) -> list[str]:
    reasons = []
    if result.selectors:
        reasons.append("diagnostic selection is not a full autotest case")
    if result.returncode:
        reasons.append(f"canonical runner exited with code {result.returncode}")
    if result.observed_workers is None:
        reasons.append("pytest worker count was not observed")
    elif result.observed_workers != result.configured_workers:
        reasons.append(f"pytest created {result.observed_workers} of {result.configured_workers} configured workers")
    if not result.cleanup_complete:
        reasons.append("run-owned Docker cleanup was not fully confirmed")
    if result.preexisting_rucio_containers:
        reasons.append("other Rucio containers were already running")
    if not result.case_identity or not result.harness_fingerprint or not result.test_manifest_fingerprint:
        reasons.append("benchmark case, harness, or test manifest identity is incomplete")
    if not all(
        (
            environment.image_id,
            environment.image_build_fingerprint,
            environment.image_os,
            environment.image_architecture,
            environment.runtime_system,
            environment.runtime_release,
            environment.runtime_machine,
            environment.runtime_python,
            environment.runtime_cpu_count,
            environment.runtime_affinity_count,
            environment.runtime_cpu_limit_source,
            environment.runtime_memory_limit_source,
            environment.xdist_version,
            environment.docker_server_version,
            environment.docker_server_os,
            environment.docker_server_kernel,
            environment.docker_server_architecture,
            environment.docker_server_cpu_count,
            environment.docker_server_memory_bytes,
            environment.dependency_images,
        )
    ):
        reasons.append("runtime or Docker server identity is incomplete")
    inspected_probe = RuntimeProbe(
        system=environment.runtime_system,
        release=environment.runtime_release,
        machine=environment.runtime_machine,
        python=environment.runtime_python,
        cpu_count=environment.runtime_cpu_count or 0,
        affinity_count=environment.runtime_affinity_count or 0,
        cpu_quota=environment.runtime_cpu_quota,
        cpu_limit_source=environment.runtime_cpu_limit_source,
        memory_limit_bytes=environment.runtime_memory_limit_bytes,
        memory_limit_source=environment.runtime_memory_limit_source,
        xdist_version=environment.xdist_version,
    )
    if (
        result.runtime_image_id != environment.image_id
        or result.dependency_images != environment.dependency_images
        or result.runtime_probe != inspected_probe
    ):
        reasons.append("captured run image manifest does not match the inspected environment")
    return reasons


def comparison_notes(environment: RuntimeEnvironment, result: RunResult) -> list[str]:
    notes = [
        "Compare a baseline and candidate only when both are marked comparable and their comparison key and test selection match.",
        "Application source is intentionally outside the comparison key so baseline and candidate code may differ; "
        "the resolved case, benchmark harness, actual test manifest, runtime build inputs, exact images, Docker "
        "resources, Python, xdist, and workers are inside it.",
        "Per-test durations overlap across workers; use pytest wall time for suite elapsed time.",
        "Effective concurrency is summed testcase time divided by pytest wall time; collection and idle time can make it less than one.",
        "The source worktree, Docker network, and Compose database volume were isolated for this run.",
    ]
    invalid_reasons = comparison_invalid_reasons(environment, result)
    if invalid_reasons:
        notes.append("This run is non-comparable: " + "; ".join(invalid_reasons) + ".")
    else:
        notes.append("This run passed the reporter's comparability checks.")
    if not result.cleanup_complete:
        notes.append("Cleanup of run-owned Docker resources could not be fully confirmed; inspect the warnings.")
    elif result.removed_residual_volume_count:
        notes.append(f"The post-run safety sweep removed {result.removed_residual_volume_count} residual project volumes.")
    else:
        notes.append("Canonical adapter cleanup completed; the post-run safety sweep found no residual project volumes.")
    if result.selectors:
        notes.append("This is a diagnostic subset, not a complete autotest benchmark.")
    if result.preexisting_rucio_containers:
        notes.append("Other Rucio containers were already running and were left untouched; their resource usage may inflate this run's timings.")
    if environment.uses_architecture_emulation:
        notes.append("The runtime image and Docker server architectures differ, so architecture emulation can inflate absolute durations. Compare this run only with another run using the same architecture pairing.")
    if result.observed_workers is None:
        notes.append("The configured worker count could not be confirmed from pytest output.")
    elif result.observed_workers != result.configured_workers:
        notes.append(f"Pytest created {result.observed_workers} workers instead of the configured {result.configured_workers}; do not use this run for controlled comparisons.")
    return notes


def environment_rows(
    environment: RuntimeEnvironment,
    result: RunResult,
    source_snapshot: SourceSnapshot,
) -> str:
    image_id = environment.image_id.removeprefix("sha256:")[:12] or "unknown"
    observed_workers = str(result.observed_workers) if result.observed_workers is not None else "unconfirmed"
    selection = "Full autotest case" if not result.selectors else f"Diagnostic: {' '.join(result.selectors)}"
    runtime_cpu = environment.runtime_cpu_count or "unknown"
    if environment.runtime_affinity_count and environment.runtime_affinity_count != environment.runtime_cpu_count:
        runtime_cpu = f"{runtime_cpu} logical, {environment.runtime_affinity_count} available"
    quota_value = f"{environment.runtime_cpu_quota:g} CPUs" if environment.runtime_cpu_quota is not None else "unlimited"
    quota = f"{quota_value} ({environment.runtime_cpu_limit_source or 'unobserved'})"
    memory_value = f"{environment.runtime_memory_limit_bytes:,} bytes" if environment.runtime_memory_limit_bytes is not None else "unlimited"
    memory_limit = f"{memory_value} ({environment.runtime_memory_limit_source or 'unobserved'})"
    docker_memory = f"{environment.docker_server_memory_bytes:,} bytes" if environment.docker_server_memory_bytes else "unknown"
    invalid_reasons = comparison_invalid_reasons(environment, result)
    rows = [
        ("Comparison key", comparison_key(environment, result)),
        ("Comparable", "no — " + "; ".join(invalid_reasons) if invalid_reasons else "yes"),
        ("Source", source_snapshot.label),
        ("Source fingerprint", f"sha256:{source_snapshot.fingerprint}"),
        ("Source role", "application candidate identity; intentionally outside the comparison key"),
        ("Benchmark harness fingerprint", f"sha256:{result.harness_fingerprint or 'unknown'}"),
        ("Test manifest fingerprint", f"sha256:{result.test_manifest_fingerprint or 'unknown'}"),
        ("Selection", selection),
        ("Workers", f"{result.configured_workers} fixed; {observed_workers} observed"),
        ("Network", f"{result.network_name} (isolated)"),
        (
            "Host",
            f"{environment.host_system} {environment.host_release}; {environment.host_machine}; {environment.host_cpu_count or 'unknown'} logical CPUs",
        ),
        (
            "Runtime",
            f"{environment.runtime_system or environment.image_os} {environment.runtime_release}; "
            f"{environment.runtime_machine or environment.image_architecture}; Python "
            f"{environment.runtime_python or 'unknown'}; {runtime_cpu} CPUs; quota {quota}; memory {memory_limit}; xdist "
            f"{environment.xdist_version or 'unknown'}",
        ),
        (
            "Docker server",
            f"{environment.docker_server_os or 'unknown'} {environment.docker_server_kernel}; "
            f"{environment.docker_server_architecture or 'unknown'}; engine {environment.docker_server_version or 'unknown'}; "
            f"{environment.docker_server_cpu_count or 'unknown'} logical CPUs; {docker_memory}",
        ),
        ("Image", f"{environment.image} ({image_id})"),
        ("Image build fingerprint", f"sha256:{environment.image_build_fingerprint}"),
        (
            "External dependency images",
            f"{len(environment.dependency_images)} services; sha256:{environment.dependency_fingerprint}",
        ),
    ]
    return "\n".join(f"<tr><th>{escape(label)}</th><td>{escape(value)}</td></tr>" for label, value in rows)


def write_metadata(
    report: JUnitReport,
    result: RunResult,
    environment: RuntimeEnvironment,
    output: Path,
    generated_at: datetime,
    source_snapshot: SourceSnapshot,
) -> None:
    total = sum(record.duration for record in report.records)
    statuses = Counter(record.status for record in report.records)
    pytest_wall_time = report.wall_time
    metadata = {
        "schema_version": 4,
        "generated_at": generated_at.isoformat(),
        "revision": source_snapshot.label,
        "source": {
            "branch": source_snapshot.branch or None,
            "head": source_snapshot.revision,
            "dirty": source_snapshot.dirty,
            "fingerprint": f"sha256:{source_snapshot.fingerprint}",
            "tracked_changes": bool(source_snapshot.patch),
            "untracked_files": len(source_snapshot.untracked_files),
            "snapshot": "current HEAD plus tracked changes and untracked non-ignored files",
        },
        "case": {
            "name": CASE_NAME,
            "distribution": "alma9",
            "python": result.case_identity.python if result.case_identity else "3.10",
            "suite": result.case_identity.suite if result.case_identity else "remote_dbs",
            "database": result.case_identity.rdbms if result.case_identity else "postgres14",
            "resolved_definition": result.case_identity.json_record() if result.case_identity else None,
        },
        "selection": {
            "kind": "diagnostic" if result.selectors else "full",
            "selectors": list(result.selectors),
        },
        "execution": {
            "command": result.command,
            "exit_code": result.returncode,
        },
        "isolation": {
            "source_checkout": "temporary-git-worktree",
            "docker_network": result.network_name,
            "compose_project": result.compose_project,
            "database_volume": "fresh-compose-project",
            "cleanup_complete": result.cleanup_complete,
            "removed_residual_volume_count": result.removed_residual_volume_count,
            "preexisting_rucio_containers": list(result.preexisting_rucio_containers),
        },
        "parallelism": {
            "mode": "fixed",
            "configured_workers": result.configured_workers,
            "observed_workers": result.observed_workers,
            "effective_concurrency": total / pytest_wall_time if pytest_wall_time else None,
        },
        "timing_seconds": {
            "pytest_wall": pytest_wall_time,
            "summed_testcases": total,
            "harness_wall": result.wall_time,
            "harness_overhead": max(0.0, result.wall_time - pytest_wall_time),
        },
        "results": {
            "tests": len(report.records),
            "passed": statuses["passed"],
            "skipped": statuses["skipped"],
            "xfailed": statuses["xfailed"],
            "xpassed": statuses["xpassed"],
            "failed": statuses["failure"],
            "errors": statuses["error"],
        },
        "host": {
            "system": environment.host_system,
            "release": environment.host_release,
            "machine": environment.host_machine,
            "logical_cpu_count": environment.host_cpu_count,
        },
        "runtime": {
            "image": environment.image,
            "image_build_fingerprint": f"sha256:{environment.image_build_fingerprint}",
            "image_id": environment.image_id,
            "image_os": environment.image_os,
            "image_architecture": environment.image_architecture,
            "system": environment.runtime_system,
            "release": environment.runtime_release,
            "machine": environment.runtime_machine,
            "python": environment.runtime_python,
            "logical_cpu_count": environment.runtime_cpu_count,
            "affinity_cpu_count": environment.runtime_affinity_count,
            "xdist_version": environment.xdist_version,
            "cpu_quota": environment.runtime_cpu_quota,
            "cpu_limit_source": environment.runtime_cpu_limit_source,
            "memory_limit_bytes": environment.runtime_memory_limit_bytes,
            "memory_limit_source": environment.runtime_memory_limit_source,
        },
        "docker_server": {
            "version": environment.docker_server_version,
            "operating_system": environment.docker_server_os,
            "kernel": environment.docker_server_kernel,
            "architecture": environment.docker_server_architecture,
            "logical_cpu_count": environment.docker_server_cpu_count,
            "memory_bytes": environment.docker_server_memory_bytes,
        },
        "dependencies": {
            "fingerprint": f"sha256:{environment.dependency_fingerprint}",
            "images": [
                {
                    "service": image.service,
                    "reference": image.reference,
                    "image_id": image.image_id,
                    "repo_digests": list(image.repo_digests),
                }
                for image in environment.dependency_images
            ],
        },
        "comparability": {
            "key": comparison_key(environment, result),
            "environment_fingerprint": f"sha256:{benchmark_environment_fingerprint(environment, result)}",
            "environment_identity": benchmark_environment_identity(environment, result),
            "harness_fingerprint": f"sha256:{result.harness_fingerprint}",
            "test_manifest_fingerprint": f"sha256:{result.test_manifest_fingerprint}",
            "application_source_fingerprint": f"sha256:{source_snapshot.fingerprint}",
            "application_source_in_key": False,
            "comparable": not comparison_invalid_reasons(environment, result),
            "invalid_reasons": comparison_invalid_reasons(environment, result),
            "architecture_emulation": environment.uses_architecture_emulation,
            "notes": comparison_notes(environment, result),
        },
    }
    write_private_text(output, json.dumps(metadata, indent=2) + "\n")


def render_html(
    report: JUnitReport,
    *,
    case: str,
    environment: RuntimeEnvironment,
    generated_at: datetime,
    source: Path,
    source_snapshot: SourceSnapshot,
    result: RunResult,
) -> str:
    records = report.records
    by_duration = sorted(records, key=lambda record: record.duration)
    slow_count = max(1, math.ceil(len(records) * SLOW_FRACTION))
    slowest = list(reversed(by_duration[-slow_count:]))
    durations = [record.duration for record in by_duration]
    total = sum(durations)
    total_tail = sum(record.duration for record in slowest)
    module_groups = grouped_tail(slowest, lambda record: record.module)
    class_groups = grouped_tail(slowest, lambda record: record.classname)
    statuses = Counter(record.status for record in records)
    failures = statuses["failure"] + statuses["error"]
    effective_concurrency = total / report.wall_time if report.wall_time else 0
    harness_overhead = max(0.0, result.wall_time - report.wall_time)
    tail_share = total_tail / total if total else 0

    chart_data = json.dumps(
        {
            "records": [record.chart_record() for record in by_duration],
            "slowCount": slow_count,
            "threshold": slowest[-1].duration,
        },
        separators=(",", ":"),
    ).replace("</", "<\\/")

    stats = "\n".join(
        [
            stat_cell("Tests", f"{len(records):,}"),
            stat_cell("Passed", f"{statuses['passed']:,}"),
            stat_cell("Skipped", f"{statuses['skipped']:,}"),
            stat_cell("Xfailed", f"{statuses['xfailed']:,}"),
            stat_cell("Xpassed", f"{statuses['xpassed']:,}"),
            stat_cell("Failed / errors", f"{failures:,}"),
            stat_cell("Comparable", "No" if comparison_invalid_reasons(environment, result) else "Yes"),
            stat_cell("Workers", f"{result.configured_workers} fixed"),
            stat_cell("Pytest wall time", format_seconds(report.wall_time)),
            stat_cell("Summed test time", format_seconds(total)),
            stat_cell("Effective concurrency", f"{effective_concurrency:.2f}x"),
            stat_cell("Harness wall time", format_seconds(result.wall_time)),
            stat_cell("Harness overhead", format_seconds(harness_overhead)),
            stat_cell("Median", format_seconds(percentile(durations, 50))),
            stat_cell("P95", format_seconds(percentile(durations, 95))),
            stat_cell("P99", format_seconds(percentile(durations, 99))),
            stat_cell("Max", format_seconds(durations[-1])),
            stat_cell("Slowest 5%", f"{slow_count:,} tests >= {format_seconds(slowest[-1].duration)}"),
            stat_cell("Tail share", f"{tail_share:.1%}"),
        ]
    )

    replacements = {
        "@@CASE@@": escape(case),
        "@@CHART_DATA@@": chart_data,
        "@@CLASS_GROUP_ROWS@@": group_rows(class_groups, slow_count, total_tail),
        "@@COMMENT@@": escape(make_comment(module_groups, class_groups, total_tail, total)),
        "@@COMPARISON_KEY@@": escape(comparison_key(environment, result)),
        "@@COMPARISON_NOTES@@": "\n".join(f"<li>{escape(note)}</li>" for note in comparison_notes(environment, result)),
        "@@COMMAND@@": escape(result.command),
        "@@ENVIRONMENT_ROWS@@": environment_rows(environment, result, source_snapshot),
        "@@EXIT_CODE@@": str(result.returncode),
        "@@GENERATED_AT@@": escape(generated_at.strftime("%Y-%m-%d %H:%M %Z")),
        "@@MODULE_GROUP_ROWS@@": group_rows(module_groups, slow_count, total_tail),
        "@@REVISION@@": escape(source_snapshot.label),
        "@@SLOW_ROWS@@": slow_rows(slowest, total),
        "@@SOURCE_NAME@@": escape(source.name),
        "@@STATS@@": stats,
    }
    html = HTML_TEMPLATE
    for placeholder, value in replacements.items():
        html = html.replace(placeholder, value)
    return html


HTML_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Rucio test runtime report</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f7f8f6;
      --ink: #18201f;
      --muted: #62706c;
      --line: #d8dfda;
      --panel: #ffffff;
      --accent: #216d68;
      --tail: #b53a34;
      --gold: #9a6b1d;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--ink);
      font: 14px/1.45 system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    main {
      width: min(1400px, calc(100vw - 32px));
      margin: 0 auto;
      padding: 24px 0 40px;
    }
    header {
      display: flex;
      align-items: end;
      justify-content: space-between;
      gap: 16px;
      margin-bottom: 18px;
    }
    h1, h2, h3 { margin: 0; line-height: 1.15; letter-spacing: 0; }
    h1 { font-size: 34px; font-weight: 750; }
    h2 { font-size: 19px; margin-bottom: 10px; }
    h3 { font-size: 15px; margin: 20px 0 8px; }
    .source {
      color: var(--muted);
      text-align: right;
      overflow-wrap: anywhere;
    }
    .command {
      margin: 8px 0 0;
      color: var(--muted);
      font: 12px/1.4 ui-monospace, SFMono-Regular, Consolas, monospace;
      overflow-wrap: anywhere;
    }
    .stats {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
      gap: 1px;
      background: var(--line);
      border: 1px solid var(--line);
      margin-bottom: 18px;
    }
    .stat { background: var(--panel); padding: 12px 14px; min-width: 0; }
    .stat span { display: block; color: var(--muted); font-size: 12px; margin-bottom: 4px; }
    .stat strong { display: block; font-size: 18px; overflow-wrap: anywhere; }
    section { background: var(--panel); border: 1px solid var(--line); margin-top: 18px; padding: 18px; }
    .chart-wrap { position: relative; min-height: 360px; }
    canvas { display: block; width: 100%; min-height: 340px; cursor: crosshair; }
    .tooltip {
      position: absolute;
      z-index: 5;
      display: none;
      max-width: min(520px, calc(100vw - 56px));
      padding: 9px 10px;
      border: 1px solid #aeb9b3;
      background: rgba(255, 255, 255, 0.97);
      box-shadow: 0 8px 24px rgba(24, 32, 31, 0.16);
      pointer-events: none;
      overflow-wrap: anywhere;
    }
    .tooltip strong { display: block; margin-bottom: 4px; }
    .comment { border-left: 4px solid var(--gold); padding-left: 12px; color: #3c3528; max-width: 980px; }
    .benchmark-notes { margin: 14px 0 0; padding: 10px 12px 10px 30px; border-left: 4px solid var(--gold); background: #faf7ef; }
    .benchmark-notes li + li { margin-top: 5px; }
    .chart-note { margin: 10px 0 0; color: var(--muted); }
    .tables {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(min(520px, 100%), 1fr));
      gap: 18px;
    }
    .table-wrap { overflow-x: auto; border: 1px solid var(--line); }
    table { width: 100%; border-collapse: collapse; background: var(--panel); min-width: 760px; }
    table.context-table { min-width: 0; }
    .context-table th { position: static; width: 170px; white-space: nowrap; }
    .context-table td { overflow-wrap: anywhere; white-space: normal; }
    th, td { padding: 8px 10px; border-bottom: 1px solid var(--line); text-align: left; vertical-align: top; }
    th {
      position: sticky;
      top: 0;
      background: #eef2ef;
      font-size: 12px;
      color: #394743;
      white-space: nowrap;
    }
    td:nth-child(-n + 6) { white-space: nowrap; }
    tbody tr:hover { background: #f3f6f4; }
    .status-failure, .status-error { color: var(--tail); font-weight: 700; }
    .status-skipped, .status-xfailed, .status-xpassed { color: var(--gold); }
    @media (max-width: 720px) {
      main { width: min(100vw - 18px, 1400px); padding-top: 12px; }
      header { align-items: start; flex-direction: column; }
      h1 { font-size: 28px; }
      .source { text-align: left; }
      section { padding: 12px; }
      table { min-width: 680px; }
    }
  </style>
</head>
<body>
  <main>
    <header>
      <div>
        <h1>Rucio test runtime report</h1>
        <div class="command">@@COMMAND@@</div>
      </div>
      <div class="source">
        Case: @@CASE@@<br>
        Revision: @@REVISION@@<br>
        Source: @@SOURCE_NAME@@<br>
        Comparison: @@COMPARISON_KEY@@<br>
        Pytest exit: @@EXIT_CODE@@<br>
        Generated: @@GENERATED_AT@@
      </div>
    </header>

    <div class="stats">
@@STATS@@
    </div>

    <section>
      <h2>Benchmark context</h2>
      <div class="table-wrap">
        <table class="context-table">
          <tbody>@@ENVIRONMENT_ROWS@@</tbody>
        </table>
      </div>
      <ul class="benchmark-notes">@@COMPARISON_NOTES@@</ul>
    </section>

    <section>
      <h2>Sorted per-test runtime profile</h2>
      <div class="chart-wrap">
        <canvas id="duration-chart" aria-label="Per-test durations ranked from fastest to slowest"></canvas>
        <div class="tooltip" id="tooltip"></div>
      </div>
      <p class="chart-note">Each point is one test measured under the configured parallel load. This is a ranked runtime profile, not a cumulative distribution.</p>
    </section>

    <section>
      <h2>Slow-tail grouping</h2>
      <p class="comment">@@COMMENT@@</p>
      <div class="tables">
        <div>
          <h3>Modules in slowest 5%</h3>
          <div class="table-wrap">
            <table>
              <thead><tr><th>Module</th><th>Tests</th><th>Tail tests</th><th>Tail time</th><th>Tail share</th><th>Max</th><th>Slowest example</th></tr></thead>
              <tbody>@@MODULE_GROUP_ROWS@@</tbody>
            </table>
          </div>
        </div>
        <div>
          <h3>Classes in slowest 5%</h3>
          <div class="table-wrap">
            <table>
              <thead><tr><th>Class</th><th>Tests</th><th>Tail tests</th><th>Tail time</th><th>Tail share</th><th>Max</th><th>Slowest example</th></tr></thead>
              <tbody>@@CLASS_GROUP_ROWS@@</tbody>
            </table>
          </div>
        </div>
      </div>
    </section>

    <section>
      <h2>Slowest 5%</h2>
      <div class="table-wrap">
        <table>
          <thead><tr><th>Rank</th><th>Time</th><th>Total share</th><th>Status</th><th>Module</th><th>Class</th><th>Test</th></tr></thead>
          <tbody>@@SLOW_ROWS@@</tbody>
        </table>
      </div>
    </section>
  </main>

  <script type="application/json" id="chart-data">@@CHART_DATA@@</script>
  <script>
    const payload = JSON.parse(document.getElementById("chart-data").textContent);
    const records = payload.records;
    const slowCount = payload.slowCount;
    const threshold = payload.threshold;
    const canvas = document.getElementById("duration-chart");
    const tooltip = document.getElementById("tooltip");
    const context = canvas.getContext("2d");
    let points = [];
    let plot = null;

    function fmtSeconds(value) {
      if (value >= 100) return value.toFixed(1) + "s";
      if (value >= 10) return value.toFixed(2) + "s";
      return value.toFixed(3) + "s";
    }

    function escapeHtml(value) {
      return String(value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;");
    }

    function draw() {
      const parentWidth = canvas.parentElement.getBoundingClientRect().width;
      const cssWidth = Math.max(320, Math.floor(parentWidth));
      const cssHeight = Math.min(620, Math.max(340, Math.round(cssWidth * 0.48)));
      const ratio = window.devicePixelRatio || 1;
      canvas.style.height = cssHeight + "px";
      canvas.width = Math.floor(cssWidth * ratio);
      canvas.height = Math.floor(cssHeight * ratio);
      context.setTransform(ratio, 0, 0, ratio, 0, 0);
      context.clearRect(0, 0, cssWidth, cssHeight);

      const margin = { top: 32, right: 28, bottom: 54, left: 68 };
      const width = cssWidth - margin.left - margin.right;
      const height = cssHeight - margin.top - margin.bottom;
      const maxDuration = records[records.length - 1].duration;
      const maxLog = Math.max(0.000001, Math.log1p(maxDuration));
      plot = { ...margin, width, height };
      points = [];

      context.fillStyle = "#ffffff";
      context.fillRect(0, 0, cssWidth, cssHeight);
      context.strokeStyle = "#d8dfda";
      context.lineWidth = 1;
      context.beginPath();
      for (let tick = 0; tick <= 4; tick += 1) {
        const y = margin.top + height - tick / 4 * height;
        context.moveTo(margin.left, y);
        context.lineTo(margin.left + width, y);
      }
      context.stroke();

      context.fillStyle = "#62706c";
      context.font = "12px system-ui, -apple-system, Segoe UI, sans-serif";
      context.textAlign = "right";
      context.textBaseline = "middle";
      for (let tick = 0; tick <= 4; tick += 1) {
        const value = Math.expm1(maxLog * tick / 4);
        const y = margin.top + height - tick / 4 * height;
        context.fillText(fmtSeconds(value), margin.left - 8, y);
      }

      const thresholdIndex = Math.max(0, records.length - slowCount);
      context.textAlign = "left";
      context.textBaseline = "top";
      context.fillText("tests ranked by duration", margin.left, margin.top + height + 16);
      context.fillText("duration (log scale)", margin.left, margin.top - 24);
      context.textAlign = "right";
      context.fillText("slowest 5%", margin.left + width, margin.top + height + 16);

      context.globalAlpha = 0.86;
      for (let index = 0; index < records.length; index += 1) {
        const record = records[index];
        const x = margin.left + (records.length === 1 ? 0 : index / (records.length - 1) * width);
        const y = margin.top + height - Math.log1p(record.duration) / maxLog * height;
        const isTail = index >= thresholdIndex;
        context.fillStyle = isTail ? "#b53a34" : "#216d68";
        context.beginPath();
        context.arc(x, y, isTail ? 2.6 : 1.8, 0, Math.PI * 2);
        context.fill();
        points.push({ x, y, index, record });
      }
      context.globalAlpha = 1;

      const thresholdX = margin.left + thresholdIndex / Math.max(1, records.length - 1) * width;
      context.strokeStyle = "#b53a34";
      context.setLineDash([4, 4]);
      context.beginPath();
      context.moveTo(thresholdX, margin.top);
      context.lineTo(thresholdX, margin.top + height);
      context.stroke();
      context.setLineDash([]);

      context.strokeStyle = "#18201f";
      context.beginPath();
      context.moveTo(margin.left, margin.top);
      context.lineTo(margin.left, margin.top + height);
      context.lineTo(margin.left + width, margin.top + height);
      context.stroke();
    }

    function nearestPoint(offsetX, offsetY) {
      if (!plot || !points.length) return null;
      const raw = (offsetX - plot.left) / plot.width * (records.length - 1);
      const center = Math.max(0, Math.min(records.length - 1, Math.round(raw)));
      let best = null;
      for (let index = Math.max(0, center - 6); index <= Math.min(points.length - 1, center + 6); index += 1) {
        const point = points[index];
        const distance = Math.hypot(point.x - offsetX, point.y - offsetY);
        if (!best || distance < best.distance) best = { ...point, distance };
      }
      return best;
    }

    canvas.addEventListener("mousemove", (event) => {
      const rect = canvas.getBoundingClientRect();
      const point = nearestPoint(event.clientX - rect.left, event.clientY - rect.top);
      if (!point) return;
      const rankFromSlowest = records.length - point.index;
      const percentile = (point.index + 1) / records.length * 100;
      tooltip.style.display = "block";
      tooltip.style.left = Math.min(event.clientX - rect.left + 14, rect.width - tooltip.offsetWidth - 6) + "px";
      tooltip.style.top = Math.max(6, event.clientY - rect.top - tooltip.offsetHeight - 12) + "px";
      tooltip.innerHTML =
        "<strong>" + escapeHtml(fmtSeconds(point.record.duration)) + " - #" + rankFromSlowest + " slowest</strong>" +
        "<div>" + escapeHtml(point.record.nodeid) + "</div>" +
        "<div>Status: " + escapeHtml(point.record.status) + "; percentile: " + percentile.toFixed(1) +
        "%; tail cutoff: " + escapeHtml(fmtSeconds(threshold)) + "</div>";
    });

    canvas.addEventListener("mouseleave", () => { tooltip.style.display = "none"; });
    window.addEventListener("resize", draw);
    draw();
  </script>
</body>
</html>
"""


def generate_report(args: argparse.Namespace) -> int:
    control_python = python_executable()
    source_snapshot = capture_source_snapshot()
    output_root = repository_path(args.output_root)
    runtime_platform = docker_server_platform()

    preexisting_rucio_containers = running_rucio_containers()
    if preexisting_rucio_containers:
        print(
            "Warning: existing Rucio containers will be left untouched but may skew timings: " + ", ".join(preexisting_rucio_containers),
            file=sys.stderr,
        )

    print(f"Source snapshot: {source_snapshot.label}")
    print(f"Source fingerprint: sha256:{source_snapshot.fingerprint}")
    with isolated_source_workspace(source_snapshot) as workspace:
        harness_fingerprint = benchmark_harness_fingerprint(workspace)
        case_identity = resolved_case_identity(workspace, control_python)
        workspace_xml = workspace / ".autotest/test-performance-output/junit.xml"
        ensure_private_directory(workspace_xml.parent, parents=True)
        write_private_text(workspace_xml, "")
        runtime_build = prepare_runtime_build(workspace, runtime_platform)

        print(f"Runtime platform: {runtime_build.platform}")
        print(f"Runtime build fingerprint: sha256:{runtime_build.fingerprint}")
        print(f"Benchmark harness fingerprint: sha256:{harness_fingerprint}")
        build_result = build_runtime_image(runtime_build)
        if build_result:
            return build_result

        result = run_tests(
            workspace,
            workspace_xml,
            args.test_selectors,
            args.workers,
            control_python,
            runtime_build.image,
            runtime_build.platform,
            preexisting_rucio_containers,
            case_identity,
            harness_fingerprint,
        )
        if not workspace_xml.is_file() or workspace_xml.stat().st_size == 0:
            print("The isolated test run did not produce JUnit XML.", file=sys.stderr)
            return result.returncode or 1
        if not result.runtime_image_id or not result.dependency_images or result.runtime_probe is None:
            raise SystemExit("The canonical test adapter did not report exact runtime, resource, and dependency identities.")
        environment = inspect_runtime_environment(
            workspace,
            runtime_build.image,
            result.runtime_image_id,
            runtime_build.fingerprint,
            result.dependency_images,
            result.runtime_probe,
        )
        print(f"Dependency image fingerprint: sha256:{environment.dependency_fingerprint}")
        run_directory = create_run_directory(output_root, f"{CASE_NAME}-w{args.workers}")
        xml_path = run_directory / "junit.xml"
        copy_private_file(workspace_xml, xml_path)

    json_path = run_directory / "timings.json"
    metadata_path = run_directory / "metadata.json"
    html_path = run_directory / "report.html"
    report = load_junit_report(xml_path)
    if not report.records:
        print(f"No <testcase> entries were found in {xml_path}", file=sys.stderr)
        return result.returncode or 1
    result = replace(result, test_manifest_fingerprint=test_manifest_fingerprint(report.records))

    generated_at = datetime.now().astimezone()
    write_json(report.records, json_path)
    write_metadata(report, result, environment, metadata_path, generated_at, source_snapshot)
    write_private_text(
        html_path,
        render_html(
            report,
            case=CASE_NAME,
            environment=environment,
            generated_at=generated_at,
            source=json_path,
            source_snapshot=source_snapshot,
            result=result,
        ),
    )

    total = sum(record.duration for record in report.records)
    statuses = Counter(record.status for record in report.records)
    print(f"Recorded {len(report.records)} tests with {total:.3f}s summed testcase time.")
    print(
        f"Status: {statuses['passed']} passed, {statuses['skipped']} skipped, "
        f"{statuses['xfailed']} xfailed, {statuses['xpassed']} xpassed, "
        f"{statuses['failure']} failed, {statuses['error']} errors."
    )
    print(f"Workers: {result.configured_workers} configured, {result.observed_workers if result.observed_workers is not None else 'unconfirmed'} observed.")
    print(f"Timing: {report.wall_time:.3f}s pytest wall, {result.wall_time:.3f}s harness wall, {total / report.wall_time if report.wall_time else 0:.2f}x effective concurrency.")
    print(f"Isolation: temporary source worktree; Docker network {result.network_name}; {result.removed_residual_volume_count} residual project volumes removed by the safety sweep; cleanup {'confirmed' if result.cleanup_complete else 'incomplete'}.")
    print(f"JUnit: {xml_path}")
    print(f"Timings: {json_path}")
    print(f"Metadata: {metadata_path}")
    print(f"HTML: {html_path}")
    print(f"Open: {html_path.as_uri()}")
    return result.returncode


def main() -> int:
    args = parse_arguments()
    verify_prerequisites()
    try:
        with termination_handler(), exclusive_run_lock():
            return generate_report(args)
    except KeyboardInterrupt:
        print("Test performance report interrupted; isolated resources were asked to stop.", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
