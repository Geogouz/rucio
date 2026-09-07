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

from fnmatch import fnmatch
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from .profiles import TestCase


case_key = pytest.StashKey["TestCase"]()
SOURCE_ROOT = Path(__file__).resolve().parents[2]


def pytest_collection_modifyitems(
    config: pytest.Config,
    items: list[pytest.Item],
) -> None:
    case = config.stash.get(case_key, None)
    if case is None:
        return

    selected = []
    deselected = []
    for item in items:
        target = selected if item_matches_case(item.path, item.nodeid, case, SOURCE_ROOT) else deselected
        target.append(item)

    if deselected:
        config.hook.pytest_deselected(items=deselected)
    items[:] = selected


def item_matches_case(
    path: "Path",
    nodeid: str,
    case: "TestCase",
    root_path: "Path",
) -> bool:
    try:
        relative = path.relative_to(root_path).as_posix()
    except ValueError:
        return False
    if any(fnmatch(relative, pattern) for pattern in case.exclude_paths):
        return False

    _, separator, node_suffix = nodeid.partition("::")
    source_nodeid = f"{relative}::{node_suffix}" if separator else relative

    for selector in case.test_paths:
        selector_path, separator, selected_node = selector.partition("::")
        normalized = selector_path.rstrip("/")
        if separator:
            expected = f"{normalized}::{selected_node}"
            if (
                source_nodeid == expected
                or source_nodeid.startswith(f"{expected}[")
                or source_nodeid.startswith(f"{expected}::")
            ):
                return True
        elif relative == normalized or relative.startswith(f"{normalized}/"):
            return True
    return False
