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
from types import SimpleNamespace

from tests.ruciopytest import collection
from tests.ruciopytest.collection import case_key, item_matches_case
from tests.ruciopytest.profiles import get_case

ROOT = Path("/source")


def test_full_suite_selects_tests_but_not_runner_tests() -> None:
    case = get_case("remote-dbs-py39-postgres14")

    assert item_matches_case(
        ROOT / "tests/test_rule.py",
        "tests/test_rule.py::test_rule",
        case,
        ROOT,
    )
    assert not item_matches_case(
        ROOT / "tests/ruciopytest/test_runner.py",
        "tests/ruciopytest/test_runner.py::test_runner",
        case,
        ROOT,
    )


def test_client_suite_selects_only_client_files() -> None:
    case = get_case("client-py39-postgres14")

    assert item_matches_case(
        ROOT / "tests/test_clients.py",
        "tests/test_clients.py::test_client",
        case,
        ROOT,
    )
    assert not item_matches_case(
        ROOT / "tests/test_rule.py",
        "tests/test_rule.py::test_rule",
        case,
        ROOT,
    )


def test_integration_suite_honours_node_selectors() -> None:
    case = get_case("integration-py39-postgres14")
    path = ROOT / "tests/test_reaper.py"

    assert item_matches_case(
        path,
        "tests/test_reaper.py::test_deletion_with_tokens",
        case,
        ROOT,
    )
    assert not item_matches_case(
        path,
        "tests/test_reaper.py::test_other_deletion",
        case,
        ROOT,
    )


def test_node_selectors_are_normalized_from_item_path() -> None:
    case = get_case("integration-py39-postgres14")
    path = ROOT / "tests/test_reaper.py"

    assert item_matches_case(
        path,
        "test_reaper.py::test_deletion_with_tokens",
        case,
        ROOT,
    )


def test_collection_uses_checkout_root(monkeypatch) -> None:
    case = get_case("client-py39-postgres14")
    item = SimpleNamespace(
        path=ROOT / "tests/test_clients.py",
        nodeid="test_clients.py::test_client",
    )
    config = SimpleNamespace(
        rootpath=ROOT / "tests",
        stash={case_key: case},
        hook=SimpleNamespace(pytest_deselected=lambda **kwargs: None),
    )
    items = [item]
    monkeypatch.setattr(collection, "SOURCE_ROOT", ROOT)

    collection.pytest_collection_modifyitems(config, items)

    assert items == [item]


def test_items_outside_checkout_do_not_match() -> None:
    assert not item_matches_case(
        Path("/outside/test_clients.py"),
        "test_clients.py::test_client",
        get_case("client-py39-postgres14"),
        ROOT,
    )


def test_integration_suite_honours_class_selectors() -> None:
    case = get_case("integration-py39-postgres14")
    path = ROOT / "tests/test_did_meta_plugins.py"

    assert item_matches_case(
        path,
        "tests/test_did_meta_plugins.py::TestDidMetaMongo::test_get_metadata",
        case,
        ROOT,
    )
