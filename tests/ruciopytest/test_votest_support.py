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

import configparser
from pathlib import Path

import pytest

from tests.ruciopytest.votest_support import (
    collect_votest_paths,
    load_matrix,
    resolve_policy,
    rewrite_policy_section,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
MATRIX_PATH = REPO_ROOT / "etc/docker/test/matrix_policy_package_tests.yml"


@pytest.mark.parametrize("policy", ["atlas", "belleii"])
def test_policy_selectors_resolve_existing_tests(policy: str) -> None:
    paths = collect_votest_paths(load_matrix(MATRIX_PATH), policy, REPO_ROOT)

    assert paths
    assert all((REPO_ROOT / path).is_file() for path in paths)


def test_policy_selector_rejects_missing_path(tmp_path: Path) -> None:
    matrix = {
        "test": {
            "tests": {
                "allow": ["rucio_tests/test_missing.py"],
                "deny": [],
            }
        }
    }

    with pytest.raises(ValueError, match="test_missing.py"):
        collect_votest_paths(matrix, "test", tmp_path)


def test_policy_selector_expands_directories(tmp_path: Path) -> None:
    tests = tmp_path / "tests"
    tests.mkdir()
    (tests / "test_selected.py").write_text("")
    (tests / "helper.py").write_text("")
    matrix = {"test": {"tests": {"allow": ["rucio_tests"], "deny": []}}}

    assert collect_votest_paths(matrix, "test", tmp_path) == [
        "tests/test_selected.py"
    ]


def test_rewrite_policy_section(tmp_path: Path) -> None:
    config_path = tmp_path / "rucio.cfg"
    config = configparser.ConfigParser()
    config["policy"] = {"stale_key": "old_value"}
    config["client"] = {"vo": "tst"}
    with config_path.open("w") as stream:
        config.write(stream)

    rewrite_policy_section(
        str(config_path),
        {"permission": "atlas", "schema": "atlas"},
    )

    actual = configparser.ConfigParser()
    actual.read(config_path)
    assert dict(actual["policy"]) == {
        "permission": "atlas",
        "schema": "atlas",
    }
    assert dict(actual["client"]) == {"vo": "tst"}


def test_rewrite_policy_section_creates_section(tmp_path: Path) -> None:
    config_path = tmp_path / "rucio.cfg"
    config_path.write_text("[client]\nvo = tst\n")

    rewrite_policy_section(str(config_path), {"permission": "belleii"})

    actual = configparser.ConfigParser()
    actual.read(config_path)
    assert dict(actual["policy"]) == {"permission": "belleii"}


class _Config:
    def __init__(self, policy: str | None) -> None:
        self.policy = policy

    def getoption(self, name: str, default=None):
        return self.policy if name == "policy" else default


def test_policy_option_precedes_environment() -> None:
    assert resolve_policy(_Config("atlas"), {"POLICY": "belleii"}) == "atlas"


def test_policy_uses_environment_fallback() -> None:
    assert resolve_policy(_Config(None), {"POLICY": "belleii"}) == "belleii"


def test_policy_can_be_unset() -> None:
    assert resolve_policy(_Config(None), {}) is None
