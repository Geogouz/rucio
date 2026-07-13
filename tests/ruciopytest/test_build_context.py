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

import subprocess  # noqa: S404
from pathlib import Path

import pytest

from tools.devenv import runtime
from tools.devenv.build_context import temporary_docker_context


def _write_inputs(root: Path, inputs: tuple[Path, ...]) -> None:
    for relative in inputs:
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"input for {relative.as_posix()}\n")


def test_temporary_context_only_stages_allowlisted_files(tmp_path: Path) -> None:
    root = tmp_path / "checkout"
    allowed = Path("requirements/required.txt")
    _write_inputs(root, (allowed,))
    (root / "requirements/untracked-sentinel.txt").write_text("must not be sent\n")
    (root / ".git").mkdir()
    (root / ".git/ignored-sentinel").write_text("must not be sent\n")

    with temporary_docker_context(root, (allowed,)) as context:
        staged = context
        files = {
            path.relative_to(context)
            for path in context.rglob("*")
            if path.is_file()
        }
        assert files == {allowed}
        assert (context / allowed).read_text() == "input for requirements/required.txt\n"

    assert not staged.exists()


def test_temporary_context_cleans_up_after_failure(tmp_path: Path) -> None:
    root = tmp_path / "checkout"
    allowed = Path("required.txt")
    _write_inputs(root, (allowed,))
    staged = None

    with pytest.raises(RuntimeError, match="build failed"):
        with temporary_docker_context(root, (allowed,)) as context:
            staged = context
            raise RuntimeError("build failed")

    assert staged is not None and not staged.exists()


def test_temporary_context_rejects_symlinked_inputs(tmp_path: Path) -> None:
    root = tmp_path / "checkout"
    root.mkdir()
    secret = tmp_path / "secret"
    secret.write_text("must not be sent\n")
    (root / "required.txt").symlink_to(secret)

    with pytest.raises(RuntimeError, match="outside the repository"):
        with temporary_docker_context(root, (Path("required.txt"),)):
            pass


def test_runtime_build_uses_allowlisted_temporary_context(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_inputs(tmp_path, runtime.RUNTIME_INPUTS)
    expected_fingerprint = runtime.runtime_fingerprint(tmp_path, "3.10", "linux/arm64", "final")
    (tmp_path / "requirements/untracked-sentinel.txt").write_text("must not be sent\n")
    assert runtime.runtime_fingerprint(tmp_path, "3.10", "linux/arm64", "final") == expected_fingerprint
    captured = {}

    def run(command, **_kwargs):
        if command[:3] == ("docker", "image", "inspect"):
            return subprocess.CompletedProcess(command, 1, stdout="")
        context = Path(command[-1])
        captured["command"] = command
        captured["context"] = context
        captured["files"] = {
            path.relative_to(context)
            for path in context.rglob("*")
            if path.is_file()
        }
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(runtime, "run", run)

    image, fingerprint, platform = runtime.ensure_runtime_image(
        tmp_path,
        environment={"RUCIO_DEV_PLATFORM": "linux/arm64"},
    )

    context = captured["context"]
    command = captured["command"]
    assert captured["files"] == set(runtime.RUNTIME_INPUTS)
    assert command[command.index("--file") + 1] == str(context / runtime.RUNTIME_DOCKERFILE)
    assert not context.exists()
    assert image.startswith("rucio-dev-runtime:py310-arm64-")
    assert fingerprint == expected_fingerprint
    assert platform == "linux/arm64"
