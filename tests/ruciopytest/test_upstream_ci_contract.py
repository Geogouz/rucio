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
from typing import TYPE_CHECKING

import pytest

validator = pytest.importorskip("tools.devenv.validate_upstream_ci_contract")

if TYPE_CHECKING:
    from pathlib import Path


def _write_contract(tmp_path: Path, protected: dict[str, str], schema: int = 1) -> Path:
    contract = tmp_path / "contract.json"
    contract.write_text(json.dumps({"schema": schema, "protected_parent_blobs": protected}))
    return contract


def test_accepts_matching_parent_blobs(tmp_path: Path) -> None:
    protected = {"workflow.yml": "workflow-blob", "runner.py": "runner-blob"}
    contract = _write_contract(tmp_path, protected)

    errors = validator.validate_upstream_ci_contract(
        tmp_path,
        contract,
        protected.__getitem__,
    )

    assert errors == []


def test_rejects_changed_parent_blobs(tmp_path: Path) -> None:
    contract = _write_contract(tmp_path, {"workflow.yml": "expected-blob"})

    errors = validator.validate_upstream_ci_contract(
        tmp_path,
        contract,
        lambda _path: "changed-blob",
    )

    assert errors == [
        "workflow.yml: parent blob changed from expected-blob to changed-blob. "
        "Audit the inherited triggers, jobs, axes, selectors, and postconditions "
        "before refreshing the contract."
    ]


@pytest.mark.parametrize(
    "document",
    (
        {"schema": 2, "protected_parent_blobs": {"workflow.yml": "blob"}},
        {"schema": 1, "protected_parent_blobs": {}},
    ),
)
def test_rejects_invalid_contracts(tmp_path: Path, document: dict[str, object]) -> None:
    contract = tmp_path / "contract.json"
    contract.write_text(json.dumps(document))

    assert validator.validate_upstream_ci_contract(tmp_path, contract, lambda _path: "blob")
