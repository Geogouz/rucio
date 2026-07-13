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

import fcntl
import hashlib
import json
import os
import secrets
import subprocess  # noqa: S404
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from pathlib import Path
    from types import TracebackType
    from typing import Optional

    from .profiles import TestCase


def checkout_id(root_dir: "Path") -> str:
    return hashlib.sha256(str(root_dir.resolve()).encode()).hexdigest()[:8]


class ContainerManager:
    COMPOSE_FILES = (
        "etc/docker/dev/docker-compose.yml",
        "etc/docker/dev/docker-compose.test.yml",
    )
    RUNTIME_DOCKERFILE = "etc/docker/test/runtime.Dockerfile"
    _locally_built_images: set[tuple[str, str]] = set()

    def __init__(
        self,
        case: "TestCase",
        root_dir: "Path",
        *,
        keep_db: bool = False,
        image: "Optional[str]" = None,
        environ: "Optional[Mapping[str, str]]" = None,
    ) -> None:
        self.case = case
        self.root_dir = root_dir.resolve()
        self.keep_db = keep_db
        self._base_environment = dict(os.environ if environ is None else environ)
        self.runtime = (
            "podman"
            if self._base_environment.get("USE_PODMAN") == "1"
            else "docker"
        )
        version_key = f"RUCIO_TEST_IMAGE_PY{case.python.replace('.', '')}"
        supplied_image = (
            image
            or self._base_environment.get(version_key)
            or self._base_environment.get("RUCIO_TEST_IMAGE")
        )
        self.image = supplied_image or (
            f"rucio-test-runtime:{checkout_id(self.root_dir)}-py{case.python.replace('.', '')}"
        )
        self.build_local = supplied_image is None
        self.project_name = self.make_project_name(
            case.id,
            self.root_dir,
            reusable=keep_db,
        )
        self.log_dir = self.root_dir / ".test-logs" / self.project_name
        self._stopped = False
        self._lock_handle = None

        self.environment = dict(self._base_environment)
        self.environment.pop("COMPOSE_PROFILES", None)
        self.environment.update(case.env_vars)
        self.environment.update({
            "RUCIO_TEST_IMAGE": self.image,
            "RUCIO_NETWORK_NAME": f"{self.project_name}-network",
            "DEV_PROFILES": ",".join(case.compose_profiles),
        })

    @staticmethod
    def make_project_name(
        case_id: str,
        root_dir: "Path",
        *,
        reusable: bool,
        nonce: "Optional[str]" = None,
    ) -> str:
        slug = "".join(
            character if character.isalnum() else "-"
            for character in case_id.lower()
        ).strip("-")
        if reusable:
            suffix = hashlib.sha256(
                str(root_dir.resolve()).encode()
            ).hexdigest()[:8]
        else:
            suffix = nonce or secrets.token_hex(4)
        return f"rucio-test-{slug[:40]}-{suffix}"

    def __enter__(self) -> "ContainerManager":
        self.start()
        return self

    def __exit__(
        self,
        exc_type: "Optional[type[BaseException]]",
        exc_value: "Optional[BaseException]",
        traceback: "Optional[TracebackType]",
    ) -> None:
        self.stop()

    def start(self) -> None:
        self._acquire_project_lock()
        try:
            if self.build_local:
                image_key = (str(self.root_dir), self.image)
                if image_key not in self._locally_built_images:
                    self._build_image()
                    self._locally_built_images.add(image_key)
            self._run(self.compose_command("up", "-d", "--wait", "--wait-timeout", "180"), timeout=240)
            self.exec(
                "rucio",
                "python",
                "-m",
                "pip",
                "install",
                "--no-deps",
                "--no-build-isolation",
                "-e",
                "/rucio_source",
                timeout=180,
            )
            self.exec("rucio", "httpd", "-k", "graceful", timeout=30)
            self.exec(
                "rucio",
                "curl",
                "--fail",
                "--silent",
                "--show-error",
                "--retry",
                "15",
                "--retry-all-errors",
                "--retry-delay",
                "2",
                "--insecure",
                "https://localhost/ping",
                timeout=60,
            )
        except BaseException:
            self.stop()
            raise

    def stop(self) -> None:
        if self._stopped:
            return
        try:
            self.capture_logs()
            command = [*self.compose_command("down", "--timeout", "30")]
            if not self.keep_db:
                command.append("--volumes")
            result = self._run(command, check=False, timeout=120)
            volumes_removed = (
                result.returncode == 0
                and (not self.keep_db or self._remove_non_database_volumes())
            )
            self._stopped = result.returncode == 0 and volumes_removed
        finally:
            self._release_project_lock()

    def compose_command(self, *arguments: str) -> list[str]:
        command = [self.runtime, "compose", "-p", self.project_name]
        for compose_file in self.COMPOSE_FILES:
            command.extend(("-f", str(self.root_dir / compose_file)))
        for profile in self.case.compose_profiles:
            command.extend(("--profile", profile))
        command.extend(arguments)
        return command

    def exec(
        self,
        service: str,
        *arguments: str,
        environment: "Optional[Mapping[str, str]]" = None,
        check: bool = True,
        capture_output: bool = False,
        interactive: bool = False,
        timeout: "Optional[int]" = None,
    ) -> subprocess.CompletedProcess:
        command = self.compose_command("exec")
        if not interactive:
            command.append("-T")
        for key, value in (environment or {}).items():
            command.extend(("--env", f"{key}={value}"))
        command.extend((service, *arguments))
        return self._run(
            command,
            check=check,
            capture_output=capture_output,
            timeout=timeout,
        )

    def capture_logs(self) -> None:
        try:
            compose = self._run(
                self.compose_command("logs", "--no-color", "--timestamps"),
                check=False,
                capture_output=True,
                timeout=60,
            )
            httpd = self.exec(
                "rucio",
                "cat",
                "/var/log/rucio/httpd_error_log",
                check=False,
                capture_output=True,
                timeout=30,
            )
            if compose.stdout or httpd.stdout:
                self.log_dir.mkdir(parents=True, exist_ok=True)
            if compose.stdout:
                (self.log_dir / "compose.log").write_text(compose.stdout)
            if httpd.stdout:
                (self.log_dir / "httpd_error.log").write_text(httpd.stdout)
        except (OSError, subprocess.SubprocessError):
            pass

    def _acquire_project_lock(self) -> None:
        if not self.keep_db or self._lock_handle is not None:
            return
        self.log_dir.mkdir(parents=True, exist_ok=True)
        lock_handle = (self.log_dir / "project.lock").open("w")
        try:
            fcntl.flock(lock_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            lock_handle.close()
            raise RuntimeError(
                f"Reusable test project {self.project_name} is already running"
            ) from error
        self._lock_handle = lock_handle

    def _release_project_lock(self) -> None:
        if self._lock_handle is None:
            return
        fcntl.flock(self._lock_handle, fcntl.LOCK_UN)
        self._lock_handle.close()
        self._lock_handle = None

    def _remove_non_database_volumes(self) -> bool:
        result = self._run(
            self.compose_command("config", "--format", "json"),
            check=False,
            capture_output=True,
            timeout=30,
        )
        if result.returncode:
            return False
        try:
            volumes = json.loads(result.stdout).get("volumes", {}).values()
        except (AttributeError, json.JSONDecodeError):
            return False
        removable = [
            volume["name"]
            for volume in volumes
            if volume.get("labels", {}).get("rucio.test.database") != self.case.rdbms
        ]
        if not removable:
            return True
        result = self._run(
            (self.runtime, "volume", "rm", *removable),
            check=False,
            timeout=60,
        )
        return result.returncode == 0

    def _build_image(self) -> None:
        if self.runtime == "docker":
            command = [
                "docker",
                "buildx",
                "build",
                "--platform",
                "linux/amd64",
                "--load",
            ]
        else:
            command = ["podman", "build", "--platform", "linux/amd64"]
        command.extend((
            "--file",
            str(self.root_dir / self.RUNTIME_DOCKERFILE),
            "--target",
            "final",
            "--build-arg",
            f"PYTHON={self.case.python}",
            "--tag",
            self.image,
            str(self.root_dir),
        ))
        self._run(command, timeout=1800)

    def _run(
        self,
        command: "Sequence[str]",
        *,
        check: bool = True,
        capture_output: bool = False,
        timeout: "Optional[int]" = None,
    ) -> subprocess.CompletedProcess:
        return subprocess.run(  # noqa: S603
            command,
            check=check,
            capture_output=capture_output,
            cwd=self.root_dir,
            env=self.environment,
            text=True,
            timeout=timeout,
        )
