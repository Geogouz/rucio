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

from contextlib import contextmanager
from typing import TYPE_CHECKING

import pytest

from tools.devenv import observe

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path


def test_main_runs_python_script_inside_named_span(monkeypatch: pytest.MonkeyPatch, tmp_path: "Path") -> None:
    result = tmp_path / "result.txt"
    script = tmp_path / "target.py"
    script.write_text(f"from pathlib import Path\nPath({str(result)!r}).write_text(__import__('sys').argv[1])\n")
    spans = []

    @contextmanager
    def capture_span(name: str, target: str, attributes: dict[str, str]) -> "Iterator[None]":
        spans.append((name, target, attributes))
        yield

    monkeypatch.setattr(observe, "_root_span", capture_span)

    assert observe.main([
        "--span-name",
        "rucio.test.run",
        "--attribute",
        "rucio.test.run_id=0123abcd",
        "--",
        "python",
        str(script),
        "complete",
    ]) == 0
    assert result.read_text() == "complete"
    assert spans == [
        ("rucio.test.run", "target.py", {"rucio.test.run_id": "0123abcd"}),
    ]


def test_main_rejects_an_invalid_span_attribute() -> None:
    with pytest.raises(SystemExit, match="2"):
        observe.main(["--attribute", "missing-value", "--", "python", "target.py"])


def test_run_python_target_resolves_console_script_and_preserves_exit(monkeypatch: pytest.MonkeyPatch, tmp_path: "Path") -> None:
    script = tmp_path / "rucio-example"
    script.write_text("#!/usr/bin/env python3\nraise SystemExit(7)\n")
    script.chmod(0o755)
    monkeypatch.setenv("PATH", str(tmp_path))

    with pytest.raises(SystemExit, match="7"):
        observe.run_python_target(["rucio-example"])


def test_run_python_target_rejects_native_commands(monkeypatch: pytest.MonkeyPatch, tmp_path: "Path") -> None:
    script = tmp_path / "native-example"
    script.write_text("#!/bin/sh\nexit 0\n")
    script.chmod(0o755)
    monkeypatch.setenv("PATH", str(tmp_path))

    with pytest.raises(ValueError, match="requires a Python target"):
        observe.run_python_target(["native-example"])


def test_run_python_target_accepts_pytest_byte_warning_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    captured = []
    monkeypatch.setattr(observe, "_run_module", lambda module, arguments: captured.append((module, arguments)) or 0)

    assert observe.run_python_target(["python", "-bb", "-m", "pytest", "test_example.py"]) == 0
    assert captured == [("pytest", ["test_example.py"])]
