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
import pstats
from html import escape
from pathlib import Path


def render(input_path: Path, output_path: Path) -> None:
    stats = pstats.Stats(str(input_path))
    rows = []
    for (filename, line, function), (primitive_calls, total_calls, own_time, cumulative_time, _callers) in stats.stats.items():
        rows.append((cumulative_time, own_time, total_calls, primitive_calls, f"{filename}:{line}({function})"))
    rows.sort(reverse=True)
    maximum = rows[0][0] if rows else 0
    body = []
    for cumulative, own, total_calls, primitive_calls, function in rows:
        width = cumulative / maximum * 100 if maximum else 0
        calls = str(total_calls) if total_calls == primitive_calls else f"{primitive_calls}/{total_calls}"
        body.append(
            "<tr>"
            f'<td class="function"><span class="bar" style="width:{width:.3f}%"></span>{escape(function)}</td>'
            f"<td>{calls}</td><td>{own:.6f}</td><td>{cumulative:.6f}</td>"
            "</tr>"
        )
    output_path.write_text(
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">"
        "<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">"
        "<title>Rucio Python call profile</title><style>"
        "body{font:14px system-ui;margin:2rem;color:#202124}h1{font-size:1.6rem}"
        "table{border-collapse:collapse;width:100%}th,td{padding:.45rem .6rem;border-bottom:1px solid #ddd;text-align:right}"
        "th{position:sticky;top:0;background:#fff}.function{position:relative;text-align:left;font-family:ui-monospace,monospace}"
        ".bar{position:absolute;inset:.2rem auto .2rem 0;background:#d9eaf7;z-index:-1}"
        "</style></head><body><h1>Python call profile</h1>"
        f"<p>{len(rows)} functions; {stats.total_calls} calls; {stats.total_tt:.6f} seconds measured.</p>"
        "<p>Rows are ordered by cumulative time. Own time excludes callees; calls show primitive/total when they differ.</p>"
        "<table><thead><tr><th class=\"function\">Function</th><th>Calls</th><th>Own seconds</th>"
        f"<th>Cumulative seconds</th></tr></thead><tbody>{''.join(body)}</tbody></table></body></html>"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Render a self-contained cProfile report.")
    parser.add_argument("input", type=Path)
    parser.add_argument("output", type=Path)
    arguments = parser.parse_args()
    render(arguments.input, arguments.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
