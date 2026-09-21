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
from pathlib import Path
from typing import TYPE_CHECKING

from .observe import run_python_target

if TYPE_CHECKING:
    from collections.abc import Sequence


def run_profile(command: "Sequence[str]", output: Path) -> int:
    import yappi

    yappi.clear_stats()
    yappi.set_clock_type("cpu")
    yappi.start(builtins=True, profile_threads=True)
    try:
        return run_python_target(command)
    finally:
        yappi.stop()
        yappi.get_func_stats().save(str(output), type="pstat")
        yappi.clear_stats()


def main(arguments: "Sequence[str] | None" = None) -> int:
    parser = argparse.ArgumentParser(description="Profile a Python target, including worker threads.")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    parsed = parser.parse_args(arguments)
    command = parsed.command[1:] if parsed.command[:1] == ["--"] else parsed.command
    if not command:
        parser.error("a Python command is required after --")
    return run_profile(command, parsed.output)


if __name__ == "__main__":
    raise SystemExit(main())
