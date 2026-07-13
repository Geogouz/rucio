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

import hashlib
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


class ContainerManager:
    COMPOSE_FILES = (
        "etc/docker/dev/docker-compose.yml",
        "etc/docker/dev/docker-compose.test.yml",
    )
    RUNTIME_DOCKERFILE = "etc/docker/test/runtime.Dockerfile"

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
        supplied_image = image or self._base_environment.get("RUCIO_TEST_IMAGE")
        self.image = supplied_image or (
            f"rucio-test-runtime:py{case.python.replace('.', '')}"
        )
        self.build_local = supplied_image is None
        self.project_name = self.make_project_name(
            case.id,
            self.root_dir,
            reusable=keep_db,
        )
        self.log_dir = self.root_dir / ".test-logs" / self.project_name
        self._stopped = False

        self.environment = dict(self._base_environment)
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
        try:
            if self.build_local:
                self._build_image()
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
        self._stopped = True
        self.capture_logs()
        command = [*self.compose_command("down", "--timeout", "30")]
        if not self.keep_db:
            command.append("--volumes")
        self._run(command, check=False, timeout=120)

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
            result = self._run(
                self.compose_command("logs", "--no-color", "--timestamps"),
                check=False,
                capture_output=True,
                timeout=60,
            )
            if result.stdout:
                self.log_dir.mkdir(parents=True, exist_ok=True)
                (self.log_dir / "compose.log").write_text(result.stdout)
        except (OSError, subprocess.SubprocessError):
            pass

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
