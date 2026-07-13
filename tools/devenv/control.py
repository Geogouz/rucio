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
import importlib.util
import os
import secrets
import shutil
import subprocess  # noqa: S404
import sys
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence


CONTROL_REQUIREMENTS = Path("requirements/requirements.devenv-control.txt")
SERVER_REQUIREMENTS = Path("requirements/requirements.server.txt")
PROJECT_METADATA = Path("pyproject.toml")
DEVELOPMENT_MARKER = ".rucio-dev-managed"
CONTROL_MARKER = ".rucio-dev-control"
MINIMUM_PYTHON = (3, 9)
ENVIRONMENT_FORMAT = b"direct-path-v1"


def _python_path(environment: Path) -> Path:
    return environment / ("Scripts/python.exe" if os.name == "nt" else "bin/python")


def _validate_host_python() -> None:
    if sys.version_info < MINIMUM_PYTHON:
        required = ".".join(str(component) for component in MINIMUM_PYTHON)
        current = ".".join(str(component) for component in sys.version_info[:3])
        raise RuntimeError(f"Python {required} or newer is required; found {current}")
    if importlib.util.find_spec("venv") is None:
        raise RuntimeError("The Python venv module is required")


def _fingerprint(root: Path, inputs: "Sequence[Path]") -> str:
    digest = hashlib.sha256()
    digest.update(ENVIRONMENT_FORMAT)
    digest.update(b"\0")
    for relative_path in inputs:
        digest.update(relative_path.as_posix().encode())
        digest.update(b"\0")
        digest.update((root / relative_path).read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def _is_current(environment: Path, marker_name: str, fingerprint: str) -> bool:
    marker = environment / marker_name
    return (
        not environment.is_symlink()
        and _python_path(environment).is_file()
        and marker.is_file()
        and marker.read_text().strip() == fingerprint
    )


def _create_environment(
    root: Path,
    environment: Path,
    marker_name: str,
    fingerprint: str,
    install_arguments: "Sequence[str]",
) -> Path:
    environment.parent.mkdir(parents=True, exist_ok=True)
    backup = environment.with_name(f"{environment.name}.backup-{secrets.token_hex(4)}")
    had_environment = environment.exists() or environment.is_symlink()
    if had_environment:
        environment.rename(backup)

    try:
        subprocess.run(  # noqa: S603
            (sys.executable, "-m", "venv", str(environment)),
            cwd=root,
            check=True,
        )
        python = _python_path(environment)
        subprocess.run(  # noqa: S603
            (
                str(python),
                "-m",
                "pip",
                "install",
                "--disable-pip-version-check",
                *install_arguments,
            ),
            cwd=root,
            check=True,
        )
        (environment / marker_name).write_text(f"{fingerprint}\n")
    except BaseException:
        if environment.is_symlink() or environment.is_file():
            environment.unlink()
        elif environment.exists():
            shutil.rmtree(environment)
        if had_environment:
            backup.rename(environment)
        raise
    else:
        if had_environment:
            shutil.rmtree(backup)
    return python


def development_environment_is_unmanaged(root: Path) -> bool:
    environment = root / ".venv"
    return (environment.exists() or environment.is_symlink()) and (
        environment.is_symlink() or not (environment / DEVELOPMENT_MARKER).is_file()
    )


def _development_python(root: Path) -> Path | None:
    environment = root / ".venv"
    if development_environment_is_unmanaged(root):
        return None

    inputs = (PROJECT_METADATA, SERVER_REQUIREMENTS, CONTROL_REQUIREMENTS)
    fingerprint = _fingerprint(root, inputs)
    if _is_current(environment, DEVELOPMENT_MARKER, fingerprint):
        return _python_path(environment)

    return _create_environment(
        root,
        environment,
        DEVELOPMENT_MARKER,
        fingerprint,
        (
            "--constraint",
            str(root / SERVER_REQUIREMENTS),
            "--requirement",
            str(root / CONTROL_REQUIREMENTS),
            "--editable",
            ".",
        ),
    )


def _control_python(root: Path) -> Path:
    environment = root / ".rucio-dev/control-venv"
    requirements = root / CONTROL_REQUIREMENTS
    fingerprint = _fingerprint(root, (CONTROL_REQUIREMENTS,))
    if _is_current(environment, CONTROL_MARKER, fingerprint):
        return _python_path(environment)

    marker = environment / CONTROL_MARKER
    if (environment.exists() or environment.is_symlink()) and (
        environment.is_symlink() or not marker.is_file()
    ):
        raise RuntimeError(f"Refusing to replace unowned control environment {environment}")

    return _create_environment(
        root,
        environment,
        CONTROL_MARKER,
        fingerprint,
        ("--requirement", str(requirements)),
    )


def control_python(root: Path) -> Path:
    _validate_host_python()
    return _development_python(root) or _control_python(root)
