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

import subprocess  # noqa: S404
from typing import TYPE_CHECKING

import pytest

from tools.devenv import prerequisites

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.parametrize(
    ("value", "expected"),
    (
        ("2.20.0", False),
        ("2.21.0", False),
        ("v2.22.0-rc.1", False),
        ("2.22.0", True),
        ("v2.22.0", True),
        ("2.22.0-desktop.1", True),
        ("2.23.0-rc.1", True),
        ("5.3.0", True),
        ("Docker Compose version v2.22.0", False),
        ("", False),
    ),
)
def test_docker_compose_version_floor(value: str, expected: bool) -> None:
    assert prerequisites.docker_compose_version_is_supported(value) is expected


def test_docker_compose_check_uses_machine_readable_version(
    tmp_path: "Path",
    monkeypatch,
) -> None:
    captured = {}

    def run(command, **kwargs):
        captured.update(command=command, options=kwargs)
        return subprocess.CompletedProcess(command, 0, stdout="2.22.0-desktop.1\n", stderr="")

    monkeypatch.setattr(prerequisites.shutil, "which", lambda name: "/usr/bin/docker")
    monkeypatch.setattr(prerequisites.subprocess, "run", run)

    assert prerequisites.check_docker_compose(tmp_path) == (True, "2.22.0-desktop.1")
    assert captured == {
        "command": ("/usr/bin/docker", "compose", "version", "--short"),
        "options": {
            "cwd": tmp_path,
            "check": False,
            "capture_output": True,
            "text": True,
        },
    }


def test_docker_compose_check_reports_missing_docker(tmp_path: "Path", monkeypatch) -> None:
    monkeypatch.setattr(prerequisites.shutil, "which", lambda name: None)
    monkeypatch.setattr(
        prerequisites.subprocess,
        "run",
        lambda *args, **kwargs: pytest.fail("subprocess should not run"),
    )

    assert prerequisites.check_docker_compose(tmp_path) == (False, "docker executable not found")


@pytest.mark.parametrize(
    ("result", "detail"),
    (
        (subprocess.CompletedProcess((), 0, stdout="2.21.0\n", stderr=""), "2.21.0"),
        (subprocess.CompletedProcess((), 1, stdout="", stderr="compose unavailable\n"), "compose unavailable"),
    ),
)
def test_docker_compose_check_reports_failure(
    tmp_path: "Path",
    monkeypatch,
    result: subprocess.CompletedProcess[str],
    detail: str,
) -> None:
    monkeypatch.setattr(prerequisites.shutil, "which", lambda name: "/usr/bin/docker")
    monkeypatch.setattr(prerequisites.subprocess, "run", lambda *args, **kwargs: result)

    passed, message = prerequisites.check_docker_compose(tmp_path)

    assert passed is False
    assert detail in message
    assert "requires 2.22.0 or newer" in message
