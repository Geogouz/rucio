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
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from tools.devenv import control

if TYPE_CHECKING:
    from collections.abc import Callable


def _repository(root: Path) -> None:
    (root / "requirements").mkdir()
    (root / "pyproject.toml").write_text("[project]\nname = 'rucio'\n")
    (root / control.SERVER_REQUIREMENTS).write_text("sqlalchemy==2.0.51\n")
    (root / control.CONTROL_REQUIREMENTS).write_text("pytest==7.4.3\n")


def _fake_environment_creator(calls: list[tuple[str, ...]]) -> "Callable[..., subprocess.CompletedProcess[str]]":
    def run(command, **kwargs) -> "subprocess.CompletedProcess[str]":
        arguments = tuple(str(argument) for argument in command)
        calls.append(arguments)
        if arguments[1:3] == ("-m", "venv"):
            python = Path(arguments[-1]) / ("Scripts/python.exe" if control.os.name == "nt" else "bin/python")
            python.parent.mkdir(parents=True)
            python.write_text("managed")
        return subprocess.CompletedProcess(command, 0)

    return run


def test_missing_development_environment_is_created_and_reused(tmp_path: Path, monkeypatch) -> None:
    _repository(tmp_path)
    calls: list[tuple[str, ...]] = []
    monkeypatch.setattr(control.subprocess, "run", _fake_environment_creator(calls))

    python = control.control_python(tmp_path)

    assert python == control._python_path(tmp_path / ".venv")
    assert python.read_text() == "managed"
    assert (tmp_path / ".venv" / control.DEVELOPMENT_MARKER).is_file()
    assert calls[0][-1] == str(tmp_path / ".venv")
    assert calls[1][0] == str(python)
    assert calls[1][4:] == (
        "--disable-pip-version-check",
        "--constraint",
        str(tmp_path / control.SERVER_REQUIREMENTS),
        "--requirement",
        str(tmp_path / control.CONTROL_REQUIREMENTS),
        "--editable",
        ".",
    )

    assert control.control_python(tmp_path) == python
    assert len(calls) == 2
    assert not list(tmp_path.glob(".venv.tmp-*"))


@pytest.mark.parametrize("changed", (control.PROJECT_METADATA, control.SERVER_REQUIREMENTS, control.CONTROL_REQUIREMENTS))
def test_development_environment_fingerprint_covers_inputs(tmp_path: Path, monkeypatch, changed: Path) -> None:
    _repository(tmp_path)
    calls: list[tuple[str, ...]] = []
    monkeypatch.setattr(control.subprocess, "run", _fake_environment_creator(calls))
    control.control_python(tmp_path)

    (tmp_path / changed).write_text((tmp_path / changed).read_text() + "# changed\n")
    control.control_python(tmp_path)

    assert len(calls) == 4
    assert not list(tmp_path.glob(".venv.backup-*"))
    assert not list(tmp_path.glob(".venv.tmp-*"))


def test_unmanaged_development_environment_is_preserved(tmp_path: Path, monkeypatch) -> None:
    _repository(tmp_path)
    existing_python = control._python_path(tmp_path / ".venv")
    existing_python.parent.mkdir(parents=True)
    existing_python.write_text("developer-owned")
    calls: list[tuple[str, ...]] = []
    monkeypatch.setattr(control.subprocess, "run", _fake_environment_creator(calls))

    python = control.control_python(tmp_path)

    assert python == control._python_path(tmp_path / ".rucio-dev/control-venv")
    assert existing_python.read_text() == "developer-owned"
    assert not (tmp_path / ".venv" / control.DEVELOPMENT_MARKER).exists()
    assert (tmp_path / ".rucio-dev/control-venv" / control.CONTROL_MARKER).is_file()


def test_failed_refresh_preserves_owned_development_environment(tmp_path: Path, monkeypatch) -> None:
    _repository(tmp_path)
    environment = tmp_path / ".venv"
    existing_python = control._python_path(environment)
    existing_python.parent.mkdir(parents=True)
    existing_python.write_text("original")
    (environment / control.DEVELOPMENT_MARKER).write_text("stale\n")

    def run(command, **kwargs):
        arguments = tuple(str(argument) for argument in command)
        if arguments[1:3] == ("-m", "venv"):
            python = control._python_path(Path(arguments[-1]))
            python.parent.mkdir(parents=True)
            python.write_text("replacement")
            return subprocess.CompletedProcess(command, 0)
        raise subprocess.CalledProcessError(1, command)

    monkeypatch.setattr(control.subprocess, "run", run)

    with pytest.raises(subprocess.CalledProcessError):
        control.control_python(tmp_path)

    assert existing_python.read_text() == "original"
    assert (environment / control.DEVELOPMENT_MARKER).read_text() == "stale\n"
    assert not list(tmp_path.glob(".venv.tmp-*"))


def test_control_python_requires_supported_python(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(control.sys, "version_info", (3, 8, 20))

    with pytest.raises(RuntimeError, match="Python 3.9 or newer"):
        control.control_python(tmp_path)


def test_control_python_requires_venv(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(control.importlib.util, "find_spec", lambda name: None)

    with pytest.raises(RuntimeError, match="venv module"):
        control.control_python(tmp_path)
