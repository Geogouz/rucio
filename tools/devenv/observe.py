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

import argparse
import re
import runpy
import shutil
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping, Sequence


def _is_python_interpreter(executable: str) -> bool:
    return re.fullmatch(r"python(?:\d+(?:\.\d+)*)?", Path(executable).name) is not None


def _resolve_python_script(executable: str) -> Path:
    path = Path(executable)
    if not path.is_file():
        resolved = shutil.which(executable)
        if resolved is None:
            raise FileNotFoundError(f"Executable not found: {executable}")
        path = Path(resolved)

    if path.suffix != ".py":
        with path.open("rb") as script:
            shebang = script.readline(256).lower()
        if not shebang.startswith(b"#!") or b"python" not in shebang:
            raise ValueError(f"Observation requires a Python target: {executable}")
    return path


def _run_module(module: str, arguments: Sequence[str]) -> int:
    original_argv = sys.argv
    sys.argv = [module, *arguments]
    try:
        runpy.run_module(module, run_name="__main__", alter_sys=True)
    finally:
        sys.argv = original_argv
    return 0


def _run_script(path: Path, arguments: Sequence[str]) -> int:
    original_argv = sys.argv
    sys.argv = [str(path), *arguments]
    try:
        runpy.run_path(str(path), run_name="__main__")
    finally:
        sys.argv = original_argv
    return 0


def run_python_target(command: Sequence[str]) -> int:
    if not command:
        raise ValueError("A Python command is required")

    executable, *arguments = command
    if _is_python_interpreter(executable):
        while arguments[:1] and arguments[0] in {"-b", "-bb"}:
            arguments.pop(0)
        if len(arguments) >= 2 and arguments[0] == "-m":
            return _run_module(arguments[1], arguments[2:])
        if arguments and not arguments[0].startswith("-"):
            return _run_script(_resolve_python_script(arguments[0]), arguments[1:])
        raise ValueError("Observation supports 'python -m MODULE' or 'python SCRIPT'")

    return _run_script(_resolve_python_script(executable), arguments)


def _target_name(command: Sequence[str]) -> str:
    if _is_python_interpreter(command[0]):
        arguments = list(command[1:])
        while arguments[:1] and arguments[0] in {"-b", "-bb"}:
            arguments.pop(0)
        if len(arguments) >= 2 and arguments[0] == "-m":
            return arguments[1]
        if arguments:
            return Path(arguments[0]).name
    return Path(command[0]).name


def _attribute(value: str) -> tuple[str, str]:
    key, separator, attribute_value = value.partition("=")
    if not separator or not key:
        raise argparse.ArgumentTypeError("attributes must use KEY=VALUE")
    return key, attribute_value


@contextmanager
def _root_span(name: str, target: str, attributes: "Mapping[str, str]") -> Iterator[None]:
    from opentelemetry import trace

    tracer = trace.get_tracer("rucio.devenv")
    span_attributes = {"rucio.dev.target": target, **attributes}
    with tracer.start_as_current_span(name, attributes=span_attributes):
        yield


def main(arguments: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run a Python target inside one OpenTelemetry root span.")
    parser.add_argument("--span-name", default="rucio.dev.run")
    parser.add_argument("--attribute", action="append", default=[], type=_attribute)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    parsed = parser.parse_args(arguments)
    command = parsed.command[1:] if parsed.command[:1] == ["--"] else parsed.command
    if not command:
        parser.error("a Python command is required after --")

    with _root_span(parsed.span_name, _target_name(command), dict(parsed.attribute)):
        return run_python_target(command)


if __name__ == "__main__":
    raise SystemExit(main())
