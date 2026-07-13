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
from typing import TYPE_CHECKING

import yaml

if TYPE_CHECKING:
    from collections.abc import Mapping
    from pathlib import Path
    from typing import Any, Optional


def load_matrix(matrix_path: "Path") -> dict:
    with matrix_path.open() as stream:
        return yaml.safe_load(stream)


def collect_votest_paths(matrix: dict, policy: str, repo_root: "Path") -> list[str]:
    def resolve(entries: list[str]) -> set[str]:
        paths: set[str] = set()
        for entry in entries:
            relative = entry.replace("rucio_tests", "tests", 1)
            path = repo_root / relative
            if path.is_dir():
                paths.update(
                    str(test.relative_to(repo_root))
                    for test in path.glob("test_*.py")
                )
            elif path.is_file():
                paths.add(relative)
            else:
                raise ValueError(
                    f"Policy {policy!r} references missing test path {entry!r}"
                )
        return paths

    tests = matrix[policy]["tests"]
    allowed = resolve(tests.get("allow", []))
    denied = resolve(tests.get("deny", []))
    return sorted(allowed - denied)


def rewrite_policy_section(rucio_cfg: str, config_overrides: "Mapping[str, Any]") -> None:
    config = configparser.ConfigParser()
    config.read(rucio_cfg)
    if "policy" not in config:
        config.add_section("policy")
    config["policy"].clear()
    for key, value in config_overrides.items():
        config["policy"][key] = str(value)
    with open(rucio_cfg, "w") as stream:
        config.write(stream)


def resolve_policy(config, env: "Mapping[str, str]") -> "Optional[str]":
    return config.getoption("policy", default=None) or env.get("POLICY")
