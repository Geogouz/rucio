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

import hashlib
import json
import os
import re
import subprocess  # noqa: S404
from pathlib import Path
from typing import TYPE_CHECKING

from .build_context import temporary_docker_context

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence


RUNTIME_DOCKERFILE = Path("etc/docker/test/runtime.Dockerfile")
RUNTIME_INPUTS = (
    RUNTIME_DOCKERFILE,
    Path("etc/docker/dev/rucio/entrypoint.sh"),
    Path("etc/docker/test/extra/httpd.conf"),
    Path("etc/docker/test/extra/rucio.conf"),
    Path("etc/docker/test/extra/00-mpm.conf"),
    Path("etc/certs/hostcert_rucio.pem"),
    Path("etc/certs/hostcert_rucio.key.pem"),
    Path("etc/certs/rucio_ca.pem"),
    Path("etc/certs/ruciouser.pem"),
    Path("etc/certs/ruciouser.key.pem"),
    Path("requirements/requirements.server.txt"),
    Path("requirements/requirements.dev.txt"),
    Path("requirements/requirements.devenv.txt"),
)


def repository_root() -> Path:
    root = Path(__file__).resolve().parents[2]
    if not (root / "pyproject.toml").is_file():
        raise RuntimeError(f"Rucio repository root not found at {root}")
    return root


def checkout_id(root: Path) -> str:
    return hashlib.sha256(str(root.resolve()).encode()).hexdigest()[:8]


def compose_project_name(root: Path) -> str:
    return f"rucio-dev-{checkout_id(root)}"


def docker_platform(root: Path, environment: "Mapping[str, str]") -> str:
    configured = environment.get("RUCIO_DEV_PLATFORM")
    if configured:
        return configured
    result = run(
        ("docker", "version", "--format", "{{.Server.Os}}/{{.Server.Arch}}"),
        root=root,
        environment=environment,
        capture_output=True,
    )
    platform = result.stdout.strip()
    if not re.fullmatch(r"[a-z0-9._-]+/[a-z0-9._-]+", platform):
        raise RuntimeError(f"Docker returned an invalid server platform: {platform!r}")
    return platform


def runtime_target(rdbms: str) -> str:
    return "oracle" if rdbms == "oracle" else "final"


def runtime_fingerprint(root: Path, python: str, platform: str, target: str) -> str:
    digest = hashlib.sha256()
    digest.update(f"rucio-dev-runtime-v1\0python={python}\0platform={platform}\0target={target}".encode())
    for configured in RUNTIME_INPUTS:
        item = root / configured
        if not item.exists() and not item.is_symlink():
            raise RuntimeError(f"Runtime input is missing: {item.relative_to(root)}")
        relative = item.relative_to(root).as_posix()
        digest.update(f"\0{relative}\0".encode())
        if item.is_symlink():
            digest.update(f"symlink:{os.readlink(item)}".encode())
        else:
            digest.update(item.read_bytes())
    return digest.hexdigest()


def runtime_image(root: Path, python: str, platform: str, target: str) -> tuple[str, str]:
    fingerprint = runtime_fingerprint(root, python, platform, target)
    architecture = platform.split("/", 1)[-1].replace("/", "-")
    image = f"rucio-dev-runtime:py{python.replace('.', '')}-{architecture}-{fingerprint[:16]}"
    return image, fingerprint


def ensure_runtime_image(
    root: Path,
    *,
    python: str = "3.10",
    rdbms: str = "postgres14",
    environment: "Mapping[str, str] | None" = None,
) -> tuple[str, str, str]:
    command_environment = dict(os.environ if environment is None else environment)
    platform = "linux/amd64" if rdbms == "oracle" else docker_platform(root, command_environment)
    target = runtime_target(rdbms)
    image, fingerprint = runtime_image(root, python, platform, target)
    inspect = run(
        (
            "docker",
            "image",
            "inspect",
            "--format",
            '{{ index .Config.Labels "org.rucio.dev.runtime-fingerprint" }}',
            image,
        ),
        root=root,
        environment=command_environment,
        check=False,
        capture_output=True,
    )
    if inspect.returncode == 0:
        if inspect.stdout.strip() != fingerprint:
            raise RuntimeError(f"Image {image} has an unexpected runtime fingerprint")
        return image, fingerprint, platform

    print(f"Building Rucio development runtime {image}", flush=True)
    with temporary_docker_context(root, RUNTIME_INPUTS) as context:
        run(
            (
                "docker",
                "buildx",
                "build",
                "--load",
                "--platform",
                platform,
                "--file",
                str(context / RUNTIME_DOCKERFILE),
                "--target",
                target,
                "--build-arg",
                f"PYTHON={python}",
                "--label",
                f"org.rucio.dev.runtime-fingerprint={fingerprint}",
                "--tag",
                image,
                str(context),
            ),
            root=root,
            environment=command_environment,
            timeout=3600,
        )
    return image, fingerprint, platform


def inspect_image(image: str, root: Path, environment: "Mapping[str, str]") -> dict[str, object]:
    result = run(
        ("docker", "image", "inspect", "--format", "{{json .}}", image),
        root=root,
        environment=environment,
        capture_output=True,
    )
    value = json.loads(result.stdout)
    return value if isinstance(value, dict) else {}


def run(
    command: "Sequence[str]",
    *,
    root: Path,
    environment: "Mapping[str, str]",
    check: bool = True,
    capture_output: bool = False,
    timeout: int | None = None,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603
        command,
        cwd=root,
        env=dict(environment),
        check=check,
        capture_output=capture_output,
        text=True,
        timeout=timeout,
    )
