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

import re
import shutil
import subprocess  # noqa: S404
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path


# docker compose pull --policy is the newest Compose feature used by the toolbox.
MINIMUM_DOCKER_COMPOSE_VERSION = (2, 22, 0)
MINIMUM_DOCKER_COMPOSE_VERSION_TEXT = ".".join(map(str, MINIMUM_DOCKER_COMPOSE_VERSION))
_COMPOSE_VERSION = re.compile(
    r"^v?(\d+)\.(\d+)\.(\d+)(?P<suffix>(?:[-+][0-9A-Za-z.-]+)?)$"
)
_PRERELEASE = re.compile(r"^(?:alpha|beta|dev|pre|preview|rc)\d*(?:\.|$)", re.IGNORECASE)


def _parse_compose_version(value: str) -> tuple[tuple[int, int, int], bool] | None:
    match = _COMPOSE_VERSION.fullmatch(value.strip())
    if match is None:
        return None
    version = (int(match.group(1)), int(match.group(2)), int(match.group(3)))
    suffix = match.group("suffix")
    prerelease = suffix.startswith("-") and _PRERELEASE.match(suffix[1:]) is not None
    return version, prerelease


def docker_compose_version_is_supported(value: str) -> bool:
    parsed = _parse_compose_version(value)
    if parsed is None:
        return False
    version, prerelease = parsed
    return version > MINIMUM_DOCKER_COMPOSE_VERSION or (
        version == MINIMUM_DOCKER_COMPOSE_VERSION and not prerelease
    )


def check_docker_compose(root: Path) -> tuple[bool, str]:
    docker = shutil.which("docker")
    if docker is None:
        return False, "docker executable not found"
    try:
        result = subprocess.run(  # noqa: S603
            (docker, "compose", "version", "--short"),
            cwd=root,
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError as error:
        return False, str(error)

    version = result.stdout.strip()
    if result.returncode == 0 and docker_compose_version_is_supported(version):
        return True, version

    detail = result.stderr.strip() or version or f"command exited with status {result.returncode}"
    return False, f"{detail}; requires {MINIMUM_DOCKER_COMPOSE_VERSION_TEXT} or newer"
