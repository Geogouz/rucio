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
import yaml

WORKFLOW_DIR = Path(__file__).resolve().parents[2] / ".github/workflows"


@pytest.mark.parametrize(
    ("filename", "permissions"),
    (
        ("unit_tests.yml", {"contents": "read"}),
        ("autotest.yml", {"contents": "read", "packages": "read"}),
        ("vo_tests.yml", {"contents": "read", "packages": "read"}),
        ("integration_tests.yml", {"contents": "read", "packages": "read"}),
    ),
)
def test_test_workflows_default_to_read_only(filename: str, permissions: dict[str, str]) -> None:
    workflow = yaml.safe_load((WORKFLOW_DIR / filename).read_text())

    assert workflow["permissions"] == permissions
