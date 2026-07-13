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
        ("unit_tests.yml", "unit", 4),
        ("autotest.yml", "autotest", 8),
        ("vo_tests.yml", "votest", 2),
        ("integration_tests.yml", "integration", 1),
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
    assert f"length == {expected_count}" in workflow
    assert "tools/test/run_tests.py" not in workflow
    assert "matrix_parser.py" not in workflow
    assert "votest_helper.py" not in workflow
    if group != "unit":
        assert "{id, python, runtime}" in workflow
        assert "fromJSON(needs.runtime_images.outputs.images)" in workflow
        assert "Select runtime image" not in workflow
