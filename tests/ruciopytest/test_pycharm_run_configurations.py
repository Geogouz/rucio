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
import os
import subprocess  # noqa: S404
import sys
from pathlib import Path
from xml.etree import ElementTree  # noqa: S405

import pytest

RUN_DIRECTORY = Path(__file__).resolve().parents[2] / ".run"
VSCODE_TASKS = RUN_DIRECTORY.parent / ".vscode/tasks.json"
PROMPT_CONFIGURATIONS = {
    "Rucio Test.run.xml": "Rucio: Test",
    "Rucio Debug Test.run.xml": "Rucio: Debug test",
    "Rucio CPU Profile.run.xml": "Rucio: CPU profile",
    "Rucio Memory Profile.run.xml": "Rucio: Memory profile",
    "Rucio Call Profile.run.xml": "Rucio: Call profile",
    "Rucio Resource Profile.run.xml": "Rucio: Resource profile",
    "Rucio Debug Daemon.run.xml": "Rucio: Debug daemon",
    "Rucio Observe Daemon.run.xml": "Rucio: Observe daemon",
    "Rucio Daemon Profile.run.xml": "Rucio: Profile daemon",
}


def _configuration_script(filename: str) -> str:
    configuration = ElementTree.parse(RUN_DIRECTORY / filename).getroot().find("configuration")  # noqa: S314
    assert configuration is not None
    assert configuration.get("type") == "ShConfigurationType"
    options = {
        option.get("name"): option.get("value", "")
        for option in configuration.findall("option")
    }
    assert options["EXECUTE_IN_TERMINAL"] == "true"
    assert options["EXECUTE_SCRIPT_FILE"] == "false"
    assert options["INTERPRETER_PATH"] == "/bin/sh"
    assert options["SCRIPT_WORKING_DIRECTORY"] == "$PROJECT_DIR$"
    script = options["SCRIPT_TEXT"]
    assert "\n" not in script
    assert script.startswith("/bin/sh -c '")
    return script


def _fake_project(tmp_path: Path) -> Path:
    project = tmp_path / "project with spaces"
    tool = project / "tools/dev"
    tool.parent.mkdir(parents=True)
    tool.write_text(
        f"#!{sys.executable}\n"
        "import json, os, sys\n"
        "with open(os.environ['RUCIO_IDE_CAPTURE'], 'w') as output:\n"
        "    json.dump(sys.argv[1:], output)\n"
    )
    tool.chmod(0o700)
    return project


def _vscode_default_argv(label: str) -> list[str]:
    configuration = json.loads(VSCODE_TASKS.read_text())
    defaults = {entry["id"]: entry["default"] for entry in configuration["inputs"]}
    task = next(task for task in configuration["tasks"] if task["label"] == label)
    assert task["type"] == "process"
    assert task["command"] == "${workspaceFolder}/tools/dev"
    return [
        defaults[argument.removeprefix("${input:").removesuffix("}")]
        if argument.startswith("${input:") and argument.endswith("}")
        else argument
        for argument in task["args"]
    ]


def _run_configuration(tmp_path: Path, filename: str, inputs: tuple[str, ...]) -> list[str]:
    project = _fake_project(tmp_path)
    capture = tmp_path / "argv.json"
    script = _configuration_script(filename)
    subprocess.run(  # noqa: S603
        ("/bin/sh", "-n", "-c", script),
        check=True,
    )
    environment = dict(os.environ)
    environment["RUCIO_IDE_CAPTURE"] = str(capture)
    subprocess.run(  # noqa: S603
        ("/bin/sh", "-c", script),
        check=True,
        input="".join(f"{value}\n" for value in inputs),
        text=True,
        env=environment,
        capture_output=True,
        cwd=project,
    )
    return json.loads(capture.read_text())


def test_pycharm_run_configurations_do_not_interpolate_prompts_into_shell() -> None:
    assert all("$Prompt" not in path.read_text() for path in RUN_DIRECTORY.glob("*.xml"))


@pytest.mark.parametrize(("filename", "vscode_label"), PROMPT_CONFIGURATIONS.items())
def test_pycharm_prompt_defaults_produce_expected_argv(
    tmp_path: Path,
    filename: str,
    vscode_label: str,
) -> None:
    assert _run_configuration(tmp_path, filename, ("", "", "")) == _vscode_default_argv(vscode_label)


def test_pycharm_prompt_values_are_passed_as_single_arguments(tmp_path: Path) -> None:
    marker = tmp_path / "injected"
    inputs = (
        "calls and resources",
        f"hermes 'quoted' \"double\" $(touch {marker})",
        "--run-once; printf injected",
    )

    assert _run_configuration(tmp_path, "Rucio Daemon Profile.run.xml", inputs) == [
        "profile",
        *inputs[:1],
        "daemon",
        *inputs[1:],
    ]
    assert not marker.exists()
