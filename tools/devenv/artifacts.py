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

import json
import math
import os
import re
import shutil
import subprocess  # noqa: S404
import threading
import uuid
from datetime import datetime, timezone
from html import escape
from pathlib import PurePosixPath
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from pathlib import Path


CONTAINER_ARTIFACT_ROOT = "/rucio_source/.rucio-dev/artifacts"
_ARTIFACT_OWNERSHIP_SCRIPT = """\
"$@" &
child=$!
forward() {
    kill "-$1" "$child" 2>/dev/null || :
}
trap 'forward INT' INT
trap 'forward TERM' TERM
trap 'forward HUP' HUP
wait "$child"
status=$?
while kill -0 "$child" 2>/dev/null; do
    wait "$child" || :
done
trap - INT TERM HUP
chown -R "${RUCIO_DEV_HOST_UID}:${RUCIO_DEV_HOST_GID}" -- "$RUCIO_DEV_ARTIFACT_DIR"
owner_status=$?
if [ "$status" -ne 0 ]; then
    exit "$status"
fi
exit "$owner_status"
"""


def artifact_ownership_environment(artifact_path: str) -> dict[str, str]:
    environment = {"RUCIO_DEV_ARTIFACT_DIR": artifact_path}
    if hasattr(os, "getuid") and hasattr(os, "getgid"):
        environment.update({
            "RUCIO_DEV_HOST_UID": str(os.getuid()),
            "RUCIO_DEV_HOST_GID": str(os.getgid()),
        })
    return environment


def artifact_ownership_command(
    command: "Sequence[str]",
    environment: "Mapping[str, str]",
) -> list[str]:
    uid = environment.get("RUCIO_DEV_HOST_UID")
    gid = environment.get("RUCIO_DEV_HOST_GID")
    if uid is None and gid is None:
        return list(command)
    artifact_dir = environment.get("RUCIO_DEV_ARTIFACT_DIR", "")
    path = PurePosixPath(artifact_dir)
    artifact_root = PurePosixPath(CONTAINER_ARTIFACT_ROOT)
    if (
        uid is None
        or gid is None
        or not uid.isdecimal()
        or not gid.isdecimal()
        or not path.is_absolute()
        or path == artifact_root
        or not path.is_relative_to(artifact_root)
        or ".." in path.parts
    ):
        raise ValueError("Invalid artifact ownership environment")
    return [
        "sh",
        "-c",
        _ARTIFACT_OWNERSHIP_SCRIPT,
        "rucio-artifact-owner",
        *command,
    ]


def _git(root: Path, *arguments: str) -> str:
    result = subprocess.run(  # noqa: S603
        (_executable("git"), *arguments),
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _executable(name: str) -> str:
    path = shutil.which(name)
    if path is None:
        raise RuntimeError(f"Required executable not found: {name}")
    return path


_SIZE_PATTERN = re.compile(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*(b|[kmgtpe]i?b)\s*$", re.IGNORECASE)
_SIZE_MULTIPLIERS = {
    "b": 1,
    "kb": 1000,
    "mb": 1000**2,
    "gb": 1000**3,
    "tb": 1000**4,
    "pb": 1000**5,
    "eb": 1000**6,
    "kib": 1024,
    "mib": 1024**2,
    "gib": 1024**3,
    "tib": 1024**4,
    "pib": 1024**5,
    "eib": 1024**6,
}


def _parse_percent(value: object) -> float | None:
    text = str(value).strip()
    if text.endswith("%"):
        text = text[:-1]
    try:
        parsed = float(text)
    except ValueError:
        return None
    return parsed if math.isfinite(parsed) else None


def _parse_bytes(value: object) -> int | None:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        parsed = float(value)
        return round(parsed) if parsed >= 0 and math.isfinite(parsed) else None
    match = _SIZE_PATTERN.fullmatch(str(value))
    if match is None:
        return None
    return round(float(match.group(1)) * _SIZE_MULTIPLIERS[match.group(2).lower()])


def _parse_io_pair(value: object) -> tuple[int | None, int | None]:
    parts = str(value).split("/", maxsplit=1)
    if len(parts) != 2:
        return None, None
    return _parse_bytes(parts[0]), _parse_bytes(parts[1])


def _parse_timestamp(value: object) -> tuple[str, int] | None:
    timestamp = str(value).strip()
    if not timestamp:
        return None
    normalized = f"{timestamp[:-1]}+00:00" if timestamp.endswith("Z") else timestamp
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return timestamp, round(parsed.timestamp() * 1000)


def _normalize_stats_record(record: dict[str, object]) -> dict[str, object] | None:
    parsed_timestamp = _parse_timestamp(record.get("timestamp", ""))
    if parsed_timestamp is None:
        return None
    timestamp, timestamp_ms = parsed_timestamp
    container = next(
        (str(record[key]).strip() for key in ("Name", "Container", "ID") if str(record.get(key, "")).strip()),
        "unknown",
    )
    memory_bytes, _memory_limit = _parse_io_pair(record.get("MemUsage", ""))
    network_rx, network_tx = _parse_io_pair(record.get("NetIO", ""))
    block_read, block_write = _parse_io_pair(record.get("BlockIO", ""))
    try:
        pids = int(str(record.get("PIDs", "")).strip())
    except ValueError:
        pids = None
    return {
        "timestamp": timestamp,
        "time_ms": timestamp_ms,
        "container": container,
        "cpu_percent": _parse_percent(record.get("CPUPerc", "")),
        "memory_bytes": memory_bytes,
        "memory_percent": _parse_percent(record.get("MemPerc", "")),
        "network_rx_bytes": network_rx,
        "network_tx_bytes": network_tx,
        "block_read_bytes": block_read,
        "block_write_bytes": block_write,
        "pids": pids,
    }


def _load_resource_samples(source: Path) -> list[dict[str, object]]:
    if not source.is_file():
        return []
    samples = []
    for line in source.read_text().splitlines():
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(record, dict):
            continue
        sample = _normalize_stats_record(record)
        if sample is not None:
            samples.append(sample)
    return sorted(samples, key=lambda sample: (int(sample["time_ms"]), str(sample["container"])))


def _write_private_text(destination: Path, content: str) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.tmp")
    try:
        temporary.write_text(content)
        os.chmod(temporary, 0o600)
        temporary.replace(destination)
        os.chmod(destination, 0o600)
    finally:
        temporary.unlink(missing_ok=True)


_RESOURCE_REPORT_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Rucio resource timeline</title>
  <style>
    :root { color-scheme: light; --bg: #f4f7f6; --panel: #fff; --ink: #18201f; --muted: #60706b; --line: #d6dfdc; --accent: #16756f; }
    * { box-sizing: border-box; }
    body { margin: 0; background: var(--bg); color: var(--ink); font: 14px/1.45 system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
    header, main { width: min(1500px, calc(100vw - 32px)); margin: 0 auto; }
    header { padding: 28px 0 12px; }
    h1, h2, h3, p { margin-top: 0; }
    h1 { margin-bottom: 6px; font-size: 32px; }
    h2 { margin-bottom: 12px; font-size: 22px; overflow-wrap: anywhere; }
    h3 { margin-bottom: 8px; font-size: 14px; }
    .eyebrow { margin-bottom: 5px; color: var(--accent); font-size: 12px; font-weight: 700; letter-spacing: .08em; text-transform: uppercase; }
    .muted { color: var(--muted); }
    .container { margin: 18px 0; padding: 18px; border: 1px solid var(--line); background: var(--panel); }
    .summary { display: grid; grid-template-columns: repeat(auto-fit, minmax(145px, 1fr)); gap: 1px; margin-bottom: 18px; border: 1px solid var(--line); background: var(--line); }
    .summary div { min-width: 0; padding: 10px 12px; background: var(--panel); }
    .summary span { display: block; color: var(--muted); font-size: 11px; text-transform: uppercase; }
    .summary strong { display: block; margin-top: 2px; overflow-wrap: anywhere; font-size: 15px; }
    .metrics { display: grid; grid-template-columns: repeat(auto-fit, minmax(min(420px, 100%), 1fr)); gap: 14px; }
    .metric { min-width: 0; padding: 12px; border: 1px solid var(--line); }
    canvas { display: block; width: 100%; height: 220px; }
    .legend { display: flex; flex-wrap: wrap; gap: 12px; min-height: 20px; color: var(--muted); font-size: 12px; }
    .legend i { display: inline-block; width: 16px; height: 3px; margin: 0 5px 3px 0; }
    .empty { padding: 24px; border: 1px solid var(--line); background: var(--panel); color: var(--muted); }
    @media (max-width: 650px) { header, main { width: min(100vw - 18px, 1500px); } h1 { font-size: 26px; } .container { padding: 12px; } }
  </style>
</head>
<body>
  <header>
    <p class="eyebrow">Rucio developer toolbox</p>
    <h1>Container resource timeline</h1>
    <p class="muted"><span id="sample-count">@@SAMPLE_COUNT@@ samples</span>. Network and block-I/O values are cumulative container counters.</p>
  </header>
  <main id="containers"></main>
  <script type="application/json" id="resource-data">@@RESOURCE_DATA@@</script>
  <script>
    "use strict";
    const samples = JSON.parse(document.getElementById("resource-data").textContent).samples;
    const containerRoot = document.getElementById("containers");
    const charts = [];
    const metrics = [
      { title: "CPU", unit: "percent", series: [{ key: "cpu_percent", label: "CPU", color: "#16756f" }] },
      { title: "Memory usage", unit: "bytes", series: [{ key: "memory_bytes", label: "Used", color: "#315da8" }] },
      { title: "Memory percentage", unit: "percent", series: [{ key: "memory_percent", label: "Used", color: "#7048a8" }] },
      { title: "Network I/O (cumulative)", unit: "bytes", series: [
        { key: "network_rx_bytes", label: "Received", color: "#137333" },
        { key: "network_tx_bytes", label: "Transmitted", color: "#c26401" }
      ] },
      { title: "Block I/O (cumulative)", unit: "bytes", series: [
        { key: "block_read_bytes", label: "Read", color: "#146c94" },
        { key: "block_write_bytes", label: "Written", color: "#b3261e" }
      ] },
      { title: "Processes", unit: "integer", series: [{ key: "pids", label: "PIDs", color: "#5f6368" }] }
    ];

    function element(name, className, text) {
      const node = document.createElement(name);
      if (className) node.className = className;
      if (text !== undefined) node.textContent = text;
      return node;
    }

    function formatBytes(value) {
      if (!Number.isFinite(value)) return "n/a";
      const units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
      let current = value;
      let index = 0;
      while (Math.abs(current) >= 1024 && index < units.length - 1) { current /= 1024; index += 1; }
      const digits = current >= 100 || index === 0 ? 0 : current >= 10 ? 1 : 2;
      return current.toFixed(digits) + " " + units[index];
    }

    function formatValue(value, unit) {
      if (!Number.isFinite(value)) return "n/a";
      if (unit === "bytes") return formatBytes(value);
      if (unit === "percent") return value.toFixed(value >= 10 ? 1 : 2) + "%";
      return Math.round(value).toString();
    }

    function drawChart(canvas, points, metric) {
      const bounds = canvas.getBoundingClientRect();
      const width = Math.max(320, Math.floor(bounds.width));
      const height = 220;
      const ratio = window.devicePixelRatio || 1;
      canvas.width = Math.floor(width * ratio);
      canvas.height = Math.floor(height * ratio);
      const context = canvas.getContext("2d");
      context.setTransform(ratio, 0, 0, ratio, 0, 0);
      context.clearRect(0, 0, width, height);
      const margin = { top: 12, right: 14, bottom: 32, left: 70 };
      const plotWidth = width - margin.left - margin.right;
      const plotHeight = height - margin.top - margin.bottom;
      const values = [];
      for (const point of points) {
        for (const series of metric.series) {
          if (Number.isFinite(point[series.key])) values.push(point[series.key]);
        }
      }
      if (!values.length) {
        context.fillStyle = "#60706b";
        context.font = "13px system-ui";
        context.fillText("No samples for this metric", margin.left, margin.top + 28);
        return;
      }
      let firstTime = Math.min(...points.map(point => point.time_ms));
      let lastTime = Math.max(...points.map(point => point.time_ms));
      if (firstTime === lastTime) { firstTime -= 500; lastTime += 500; }
      const observedMaximum = Math.max(...values);
      const maximum = Math.max(metric.unit === "integer" ? 1 : 0.01, observedMaximum * 1.08);
      const x = value => margin.left + (value - firstTime) / (lastTime - firstTime) * plotWidth;
      const y = value => margin.top + plotHeight - value / maximum * plotHeight;

      context.strokeStyle = "#d6dfdc";
      context.lineWidth = 1;
      context.beginPath();
      for (let tick = 0; tick <= 4; tick += 1) {
        const yPosition = margin.top + tick / 4 * plotHeight;
        context.moveTo(margin.left, yPosition);
        context.lineTo(margin.left + plotWidth, yPosition);
      }
      context.stroke();
      context.fillStyle = "#60706b";
      context.font = "11px system-ui";
      context.textAlign = "right";
      context.fillText(formatValue(maximum, metric.unit), margin.left - 7, margin.top + 4);
      context.fillText(formatValue(0, metric.unit), margin.left - 7, margin.top + plotHeight + 4);
      context.textAlign = "left";
      context.fillText(new Date(firstTime).toLocaleTimeString(), margin.left, height - 8);
      context.textAlign = "right";
      context.fillText(new Date(lastTime).toLocaleTimeString(), width - margin.right, height - 8);

      for (const series of metric.series) {
        context.strokeStyle = series.color;
        context.lineWidth = 2;
        context.beginPath();
        let started = false;
        let plotted = 0;
        let pointX = 0;
        let pointY = 0;
        for (const point of points) {
          const value = point[series.key];
          if (!Number.isFinite(value)) { started = false; continue; }
          pointX = x(point.time_ms);
          pointY = y(value);
          plotted += 1;
          if (started) context.lineTo(pointX, pointY);
          else { context.moveTo(pointX, pointY); started = true; }
        }
        context.stroke();
        if (plotted === 1) {
          context.fillStyle = series.color;
          context.beginPath();
          context.arc(pointX, pointY, 3, 0, 2 * Math.PI);
          context.fill();
        }
      }
    }

    function summaryItem(label, value) {
      const item = element("div");
      item.append(element("span", "", label), element("strong", "", value));
      return item;
    }

    function renderContainer(name, points) {
      const card = element("section", "container");
      card.append(element("h2", "", name));
      const latest = points[points.length - 1];
      const summary = element("div", "summary");
      summary.append(
        summaryItem("Last sample", new Date(latest.time_ms).toLocaleString()),
        summaryItem("CPU", formatValue(latest.cpu_percent, "percent")),
        summaryItem("Memory", formatBytes(latest.memory_bytes)),
        summaryItem("Memory %", formatValue(latest.memory_percent, "percent")),
        summaryItem("Network RX / TX", formatBytes(latest.network_rx_bytes) + " / " + formatBytes(latest.network_tx_bytes)),
        summaryItem("Block read / write", formatBytes(latest.block_read_bytes) + " / " + formatBytes(latest.block_write_bytes)),
        summaryItem("PIDs", formatValue(latest.pids, "integer"))
      );
      card.append(summary);
      const grid = element("div", "metrics");
      for (const metric of metrics) {
        const panel = element("section", "metric");
        panel.append(element("h3", "", metric.title));
        const legend = element("div", "legend");
        for (const series of metric.series) {
          const entry = element("span");
          const marker = element("i");
          marker.style.backgroundColor = series.color;
          entry.append(marker, document.createTextNode(series.label));
          legend.append(entry);
        }
        panel.append(legend);
        const canvas = element("canvas");
        canvas.setAttribute("role", "img");
        canvas.setAttribute("aria-label", metric.title + " over time for " + name);
        panel.append(canvas);
        grid.append(panel);
        charts.push(() => drawChart(canvas, points, metric));
      }
      card.append(grid);
      containerRoot.append(card);
    }

    if (!samples.length) {
      containerRoot.append(element("p", "empty", "No Docker resource samples were recorded for this run."));
    } else {
      const groups = new Map();
      for (const sample of samples) {
        if (!groups.has(sample.container)) groups.set(sample.container, []);
        groups.get(sample.container).push(sample);
      }
      for (const [name, points] of [...groups.entries()].sort((left, right) => left[0].localeCompare(right[0]))) {
        points.sort((left, right) => left.time_ms - right.time_ms);
        renderContainer(name, points);
      }
      for (const draw of charts) draw();
      let resizeTimer;
      window.addEventListener("resize", () => {
        window.clearTimeout(resizeTimer);
        resizeTimer = window.setTimeout(() => { for (const draw of charts) draw(); }, 100);
      });
    }
  </script>
</body>
</html>
"""


def _render_resource_report(samples: "Sequence[dict[str, object]]") -> str:
    data = json.dumps({"samples": samples}, separators=(",", ":"), sort_keys=True)
    data = data.replace("&", "\\u0026").replace("<", "\\u003c").replace(">", "\\u003e")
    return _RESOURCE_REPORT_TEMPLATE.replace("@@SAMPLE_COUNT@@", str(len(samples))).replace("@@RESOURCE_DATA@@", data)


def _improve_cpu_flamegraph(path: Path) -> None:
    if not path.is_file():
        return
    content = path.read_text()
    if 'id="search-background"' in content:
        return
    search = re.search(r'<text id="search"(?P<attributes>[^>]*)>Search</text>', content)
    if search is None:
        return
    x_match = re.search(r'\bx="(?P<value>[0-9.]+)"', search.group("attributes"))
    y_match = re.search(r'\by="(?P<value>[0-9.]+)"', search.group("attributes"))
    if x_match is None or y_match is None or "</style>" not in content:
        return

    x = float(x_match.group("value"))
    y = float(y_match.group("value"))
    background = (
        f'<rect id="search-background" x="{x - 96:g}" y="{y - 16:g}" '
        'width="102" height="21" rx="3"/>'
    )
    content = content.replace(search.group(0), background + search.group(0), 1)
    content = content.replace(
        "</style>",
        """#search-background { fill:#fff; fill-opacity:0.94; stroke:#5f6368; stroke-width:0.75; pointer-events:none; }
#search { opacity:1; font-weight:bold; }
</style>""",
        1,
    )
    resize_marker = "searchbtn.attributes.x.value = svgWidth - xpad;"
    if resize_marker in content:
        content = content.replace(
            resize_marker,
            resize_marker
            + '\n            document.getElementById("search-background").attributes.x.value = svgWidth - xpad - 96;',
            1,
        )
    _write_private_text(path, content)


class ArtifactRun:
    def __init__(
        self,
        root: Path,
        *,
        name: str,
        command: "Sequence[str]",
        image: str = "",
        platform: str = "",
        project: str = "",
    ) -> None:
        self.root = root
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        slug = "".join(character if character.isalnum() else "-" for character in name.lower()).strip("-")
        artifacts_root = root / ".rucio-dev/artifacts"
        artifacts_root.mkdir(parents=True, exist_ok=True)
        os.chmod(root / ".rucio-dev", 0o700)
        os.chmod(artifacts_root, 0o700)
        self.path = artifacts_root / f"{timestamp}-{slug}-{uuid.uuid4().hex[:8]}"
        self.path.mkdir()
        os.chmod(self.path, 0o700)
        self.incomplete = self.path / ".incomplete"
        _write_private_text(self.incomplete, "running\n")
        self.started = datetime.now(timezone.utc)
        self.manifest: dict[str, object] = {
            "schema_version": 1,
            "name": name,
            "command": list(command),
            "started_at": self.started.isoformat(),
            "git": {
                "revision": _git(root, "rev-parse", "HEAD"),
                "branch": _git(root, "branch", "--show-current"),
                "dirty": bool(_git(root, "status", "--porcelain")),
            },
            "runtime": {
                "image": image,
                "platform": platform,
                "compose_project": project,
            },
            "artifacts": [],
        }
        self.write_manifest()

    @property
    def container_path(self) -> str:
        return f"/rucio_source/{self.path.relative_to(self.root).as_posix()}"

    @property
    def container_environment(self) -> dict[str, str]:
        return artifact_ownership_environment(self.container_path)

    def finish(self, returncode: int) -> None:
        finished = datetime.now(timezone.utc)
        self.manifest["finished_at"] = finished.isoformat()
        self.manifest["duration_seconds"] = round((finished - self.started).total_seconds(), 3)
        self.manifest["returncode"] = returncode
        _improve_cpu_flamegraph(self.path / "cpu.svg")
        samples = _load_resource_samples(self.path / "docker-stats.jsonl")
        _write_private_text(self.path / "resources.html", _render_resource_report(samples))
        self.manifest["artifacts"] = sorted(item.relative_to(self.path).as_posix() for item in self.path.rglob("*") if item.is_file() and item.name not in {".incomplete", "manifest.json", "index.html"})
        self.write_manifest()
        self.write_index()
        self.incomplete.unlink(missing_ok=True)
        self._make_private()

    def write_manifest(self) -> None:
        _write_private_text(
            self.path / "manifest.json",
            json.dumps(self.manifest, indent=2, sort_keys=True) + "\n",
        )

    def write_index(self) -> None:
        links = []
        for name in self.manifest.get("artifacts", []):
            label = escape(str(name))
            links.append(f'<li><a href="{label}">{label}</a></li>')
        runtime = self.manifest.get("runtime", {})
        html = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>Rucio development report</title>
<style>body{{font:16px system-ui;max-width:960px;margin:3rem auto;padding:0 1rem;color:#202124}}code{{background:#f1f3f4;padding:.15rem .35rem}}li{{margin:.5rem 0}}.ok{{color:#137333}}.bad{{color:#b3261e}}</style></head>
<body><h1>{escape(str(self.manifest["name"]))}</h1>
<p class="{"ok" if self.manifest.get("returncode") == 0 else "bad"}">Exit code: {self.manifest.get("returncode")}</p>
<p>Runtime: <code>{escape(str(runtime))}</code></p>
<h2>Artifacts</h2><ul>{"".join(links)}</ul>
<p>Use <code>manifest.json</code> for the exact source, runtime, arguments and duration.</p></body></html>"""
        _write_private_text(self.path / "index.html", html)

    def _make_private(self) -> None:
        for item in self.path.rglob("*"):
            if item.is_symlink():
                continue
            os.chmod(item, 0o700 if item.is_dir() else 0o600)


class DockerStatsSampler:
    def __init__(self, root: Path, project: str, output: Path, *, interval_seconds: float = 1) -> None:
        if interval_seconds <= 0:
            raise ValueError("Docker stats interval must be positive")
        self.root = root
        self.project = project
        self.output = output
        self.interval_seconds = interval_seconds
        self._stop = threading.Event()
        self._initial_sample = threading.Event()
        self._thread = threading.Thread(target=self._sample, daemon=True)

    def __enter__(self) -> "DockerStatsSampler":
        self._thread.start()
        self._initial_sample.wait()
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        self._stop.set()
        self._thread.join()

    def _sample(self) -> None:
        try:
            self._sample_once()
        finally:
            self._initial_sample.set()
        while not self._stop.wait(self.interval_seconds):
            self._sample_once()

    def _sample_once(self) -> None:
        docker = _executable("docker")
        try:
            containers = subprocess.run(  # noqa: S603
                (
                    docker,
                    "ps",
                    "--filter",
                    f"label=com.docker.compose.project={self.project}",
                    "--format",
                    "{{.ID}}",
                ),
                cwd=self.root,
                check=False,
                capture_output=True,
                text=True,
                timeout=5,
            ).stdout.split()
        except (OSError, subprocess.TimeoutExpired):
            return
        if not containers:
            return
        try:
            result = subprocess.run(  # noqa: S603
                (docker, "stats", "--no-stream", "--format", "{{json .}}", *containers),
                cwd=self.root,
                check=False,
                capture_output=True,
                text=True,
                timeout=5,
            )
        except (OSError, subprocess.TimeoutExpired):
            return
        if result.returncode:
            return
        timestamp = datetime.now(timezone.utc).isoformat()
        records = []
        for line in result.stdout.splitlines():
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(record, dict):
                continue
            record["timestamp"] = timestamp
            records.append(json.dumps(record, sort_keys=True) + "\n")
        if not records:
            return
        self.output.parent.mkdir(parents=True, exist_ok=True)
        flags = os.O_APPEND | os.O_CREAT | os.O_WRONLY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(self.output, flags, 0o600)
        if hasattr(os, "fchmod"):
            os.fchmod(descriptor, 0o600)
        else:
            os.chmod(self.output, 0o600)
        with os.fdopen(descriptor, "a", encoding="utf-8") as output:
            output.writelines(records)
