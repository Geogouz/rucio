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

import os
import stat
import subprocess  # noqa: S404
from contextlib import contextmanager
from typing import TYPE_CHECKING

from .runtime import compose_project_name, ensure_runtime_image

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping, Sequence
    from io import TextIOWrapper
    from pathlib import Path


WORKSPACE_PROFILE = "workspace"
DAEMON_PROFILES = {
    "rucio-abacus-account": (),
    "rucio-abacus-collection-replica": (),
    "rucio-abacus-rse": (),
    "rucio-atropos": (),
    "rucio-auditor": ("storage",),
    "rucio-automatix": ("storage",),
    "rucio-bb8": (),
    "rucio-cache-consumer": ("messaging",),
    "rucio-conveyor-finisher": (),
    "rucio-conveyor-poller": ("storage",),
    "rucio-conveyor-preparer": (),
    "rucio-conveyor-receiver": ("messaging",),
    "rucio-conveyor-stager": ("storage",),
    "rucio-conveyor-submitter": ("storage",),
    "rucio-conveyor-throttler": (),
    "rucio-dark-reaper": ("storage",),
    "rucio-dumper": ("storage",),
    "rucio-follower": (),
    "rucio-hermes": ("messaging",),
    "rucio-judge-cleaner": (),
    "rucio-judge-evaluator": (),
    "rucio-judge-injector": (),
    "rucio-judge-repairer": (),
    "rucio-kronos": ("messaging",),
    "rucio-minos": (),
    "rucio-minos-temporary-expiration": (),
    "rucio-necromancer": (),
    "rucio-reaper": ("storage",),
    "rucio-replica-recoverer": (),
    "rucio-rse-decommissioner": (),
    "rucio-storage-consistency-actions": ("storage",),
    "rucio-transmogrifier": (),
    "rucio-undertaker": (),
}


@contextmanager
def workspace_lock(root: Path) -> "Iterator[None]":
    """Prevent concurrent commands from mutating the shared workspace."""
    try:
        import fcntl
    except ImportError as error:  # pragma: no cover - native Windows is unsupported
        raise RuntimeError("Workspace locking requires a POSIX host or WSL") from error

    with _open_workspace_lock(root) as lock_file:
        try:
            fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            lock_file.seek(0)
            owner = lock_file.read().strip()
            detail = f" (PID {owner})" if owner else ""
            raise RuntimeError(
                f"Rucio workspace is busy{detail}; stop the active workspace command first"
            ) from error
        lock_file.seek(0)
        lock_file.truncate()
        lock_file.write(str(os.getpid()))
        lock_file.flush()
        try:
            yield
        finally:
            fcntl.flock(lock_file, fcntl.LOCK_UN)


def _open_workspace_lock(root: Path) -> "TextIOWrapper":
    lock_dir = root.resolve() / ".rucio-dev"
    lock_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    directory_flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_DIRECTORY", 0)
    directory_flags |= getattr(os, "O_NOFOLLOW", 0)
    directory_fd: int | None = None
    lock_fd: int | None = None
    try:
        directory_fd = os.open(lock_dir, directory_flags)
        directory_stat = os.fstat(directory_fd)
        if not stat.S_ISDIR(directory_stat.st_mode) or directory_stat.st_uid != os.getuid():
            raise RuntimeError("The Rucio workspace lock directory must be owned by the current user")
        os.fchmod(directory_fd, 0o700)

        lock_flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0)
        lock_flags |= getattr(os, "O_NOFOLLOW", 0)
        lock_fd = os.open("workspace.lock", lock_flags, 0o600, dir_fd=directory_fd)
        lock_stat = os.fstat(lock_fd)
        path_stat = os.stat("workspace.lock", dir_fd=directory_fd, follow_symlinks=False)
        if not stat.S_ISREG(lock_stat.st_mode):
            raise RuntimeError("The Rucio workspace lock must be a regular file")
        if lock_stat.st_nlink != 1:
            raise RuntimeError("The Rucio workspace lock must be a single-link file")
        if lock_stat.st_uid != os.getuid():
            raise RuntimeError("The Rucio workspace lock must be owned by the current user")
        if (lock_stat.st_dev, lock_stat.st_ino) != (path_stat.st_dev, path_stat.st_ino):
            raise RuntimeError("The Rucio workspace lock changed while it was opened")
        os.fchmod(lock_fd, 0o600)
        lock_file = os.fdopen(lock_fd, "r+", encoding="ascii")
        lock_fd = None
        return lock_file
    except OSError as error:
        raise RuntimeError("Cannot safely open the Rucio workspace lock") from error
    finally:
        if lock_fd is not None:
            os.close(lock_fd)
        if directory_fd is not None:
            os.close(directory_fd)


class ComposeProject:
    def __init__(
        self,
        root: Path,
        *,
        profiles: "Sequence[str]" = (),
        ports: bool = False,
        debug: bool = False,
        api: bool = False,
        instrumentation: bool = False,
        observe: bool = False,
        build: bool = True,
        environment: "Mapping[str, str] | None" = None,
    ) -> None:
        self.root = root.resolve()
        self.project_name = compose_project_name(self.root)
        self.profiles = tuple(dict.fromkeys(profiles))
        self.environment = dict(os.environ if environment is None else environment)
        self.environment.pop("COMPOSE_PROFILES", None)
        self.environment.pop("DOCKER_DEFAULT_PLATFORM", None)
        if build:
            image, fingerprint, platform = ensure_runtime_image(self.root, environment=self.environment)
        else:
            image = self.environment.get("RUCIO_DEV_IMAGE", "rucio-dev-runtime:unused")
            fingerprint = ""
            platform = self.environment.get("RUCIO_DEV_PLATFORM", "linux/amd64")
        self.image = image
        self.fingerprint = fingerprint
        self.platform = platform
        self.environment.update(
            {
                "DEV_PROFILES": ",".join(self.profiles),
                "RDBMS": "postgres14",
                "RUCIO_DEV_IMAGE": image,
                "RUCIO_DEV_PLATFORM": platform,
                "RUCIO_NETWORK_NAME": f"{self.project_name}-network",
            }
        )
        files = [self.root / "etc/docker/dev/docker-compose.yml"]
        if instrumentation:
            files.append(self.root / "etc/docker/dev/docker-compose.instrumentation.yml")
        if debug:
            files.append(self.root / "etc/docker/dev/docker-compose.debug.yml")
        if api:
            files.append(self.root / "etc/docker/dev/docker-compose.api.yml")
        if observe:
            files.append(self.root / "etc/docker/dev/docker-compose.observe.yml")
        if ports:
            files.append(self.root / "etc/docker/dev/docker-compose.ports.yml")
        self.files = tuple(files)

    def command(self, *arguments: str) -> list[str]:
        command = ["docker", "compose", "--project-name", self.project_name]
        for compose_file in self.files:
            command.extend(("--file", str(compose_file)))
        command.extend(("--profile", WORKSPACE_PROFILE))
        for profile in self.profiles:
            command.extend(("--profile", profile))
        command.extend(arguments)
        return command

    def run(
        self,
        *arguments: str,
        check: bool = True,
        capture_output: bool = False,
        timeout: int | None = None,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(  # noqa: S603
            self.command(*arguments),
            cwd=self.root,
            env=self.environment,
            check=check,
            capture_output=capture_output,
            text=True,
            timeout=timeout,
        )

    def up(self, *, initialize: bool = True) -> None:
        self.pull_dependencies()
        self.run("up", "--detach", "--wait", "--wait-timeout", "180", timeout=240)
        self._install_workspace()
        case = self._workspace_case()
        if initialize:
            self.exec(
                "rucio",
                "python",
                "-m",
                "tests.ruciopytest.infra_manager",
                "--case",
                case,
                "--keep-db",
                timeout=300,
            )
        self._prepare_workspace_runtime(case)

    def recreate_workspace(self) -> None:
        """Recreate the workspace from the base model without touching shared services."""
        self.run(
            "up",
            "--detach",
            "--wait",
            "--wait-timeout",
            "180",
            "--no-deps",
            "--force-recreate",
            "workspace",
            timeout=240,
        )
        self._install_workspace()
        self._prepare_workspace_runtime(self._workspace_case())

    def _install_workspace(self) -> None:
        self.exec(
            "workspace",
            "python",
            "-m",
            "pip",
            "install",
            "--no-deps",
            "--no-build-isolation",
            "--editable",
            "/rucio_source",
            timeout=180,
        )
        self.exec("workspace", "python", "-m", "pip", "check", timeout=60)

    def _workspace_case(self) -> str:
        if "storage" in self.profiles:
            return "integration-py39-postgres14"
        return "remote-dbs-py310-postgres14"

    def _prepare_workspace_runtime(self, case: str) -> None:
        self.exec(
            "workspace",
            "python",
            "-m",
            "tests.ruciopytest.infra_manager",
            "--case",
            case,
            "--runtime-only",
            timeout=60,
        )

    def pull_dependencies(self) -> None:
        services = self.run("config", "--services", capture_output=True, timeout=30).stdout.split()
        dependencies = [service for service in services if service not in {"rucio", "workspace"}]
        if dependencies:
            self.run("pull", "--policy", "always", *dependencies, timeout=900)

    def exec(
        self,
        service: str,
        *arguments: str,
        interactive: bool = False,
        check: bool = True,
        capture_output: bool = False,
        timeout: int | None = None,
    ) -> subprocess.CompletedProcess[str]:
        command = ["exec"]
        if not interactive:
            command.append("--no-TTY")
        command.extend((service, *arguments))
        return self.run(*command, check=check, capture_output=capture_output, timeout=timeout)


def profiles_for_daemon(name: str) -> tuple[str, ...]:
    executable = name if name.startswith("rucio-") else f"rucio-{name}"
    return DAEMON_PROFILES.get(executable, ())
