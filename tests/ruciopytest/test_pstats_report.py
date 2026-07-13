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

import cProfile
from typing import TYPE_CHECKING

from tools.devenv.pstats_report import render

if TYPE_CHECKING:
    from pathlib import Path


def test_render_creates_self_contained_call_report(tmp_path: "Path") -> None:
    capture = tmp_path / "calls.pstats"
    output = tmp_path / "calls.html"
    cProfile.runctx("sum(range(10))", {}, {}, str(capture))

    render(capture, output)

    html = output.read_text()
    assert "Python call profile" in html
    assert "Cumulative seconds" in html
    assert "sum" in html
    assert "http://" not in html
    assert "https://" not in html
