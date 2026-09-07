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

import sys
from types import SimpleNamespace
from typing import TYPE_CHECKING

from tools.devenv import calls

if TYPE_CHECKING:
    from pathlib import Path


def test_profile_includes_threads_and_writes_pstats(monkeypatch, tmp_path: "Path") -> None:
    events = []

    class Stats:
        @staticmethod
        def save(path: str, *, type: str) -> None:
            events.append(("save", path, type))

    profiler = SimpleNamespace(
        clear_stats=lambda: events.append(("clear",)),
        set_clock_type=lambda clock: events.append(("clock", clock)),
        start=lambda **kwargs: events.append(("start", kwargs)),
        stop=lambda: events.append(("stop",)),
        get_func_stats=lambda: Stats(),
    )
    monkeypatch.setitem(sys.modules, "yappi", profiler)
    monkeypatch.setattr(calls, "run_python_target", lambda command: events.append(("run", tuple(command))) or 0)
    output = tmp_path / "calls.pstats"

    assert calls.main(["--output", str(output), "--", "python", "target.py"]) == 0
    assert events == [
        ("clear",),
        ("clock", "cpu"),
        ("start", {"builtins": True, "profile_threads": True}),
        ("run", ("python", "target.py")),
        ("stop",),
        ("save", str(output), "pstat"),
        ("clear",),
    ]
