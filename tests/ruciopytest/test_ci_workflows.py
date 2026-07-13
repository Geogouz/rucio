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

from pathlib import Path

import pytest

WORKFLOW_DIR = Path(__file__).resolve().parents[2] / ".github/workflows"


@pytest.mark.parametrize(
    ("filename", "group", "expected_count"),
    (
        ("unit_tests.yml", "unit", 5),
        ("autotest.yml", "autotest", 20),
        ("vo_tests.yml", "votest", 10),
        ("integration_tests.yml", "integration", 5),
    ),
)
def test_ci_matrices_use_canonical_cases(
    filename: str,
    group: str,
    expected_count: int,
) -> None:
    workflow = (WORKFLOW_DIR / filename).read_text()

    assert "python -bb -m pytest --list-cases" in workflow
    assert f'.group == "{group}"' in workflow
    assert "--case=" in workflow
    assert f"length >= {expected_count}" in workflow
    assert "tools/test/run_tests.py" not in workflow
    assert "matrix_parser.py" not in workflow
    assert "votest_helper.py" not in workflow
    if group != "unit":
        if group == "autotest":
            assert "runtime_target" in workflow
            assert "runtime_platform" in workflow
        else:
            assert "{id, python, runtime}" in workflow
        assert "fromJSON(needs.runtime_images.outputs.images)" in workflow
        assert "fromJSON(needs.runtime_images.outputs.builds)" in workflow
        assert "packages: read" in workflow
        assert "packages: write" not in workflow
        assert "Select runtime image" not in workflow
        build = workflow.index("- name: Build runtime image locally")
        prune = workflow.index("- name: Prune runtime build cache")
        run = workflow.index("- name: Run ${{ matrix.case.id }}")
        assert build < prune < run
        assert "docker buildx prune --all --force" in workflow
    if group == "autotest":
        assert "target: ${{ matrix.case.runtime_target }}" in workflow
        assert "platforms: ${{ matrix.case.runtime_platform }}" in workflow


def test_autotest_preserves_upstream_non_matrix_assertions() -> None:
    workflow = (WORKFLOW_DIR / "autotest.yml").read_text()

    assert "pull_request:" in workflow
    assert "push:" in workflow
    assert "schedule:" in workflow
    assert "name: Lint (pre-commit)" in workflow
    assert "name: Check Python Type Annotations" in workflow
    assert "Compare numbers of missing type annotations" in workflow
    assert "name: Python type check (Pyright)" in workflow
    assert "tools/run_pyright.sh compare --Werror" in workflow
