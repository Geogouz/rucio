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
import subprocess  # noqa: S404
import sys
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
CONTRACT = REPOSITORY_ROOT / ".github/upstream-ci-contract.json"


def _parent_blob(repository_root: Path, path: str) -> str:
    result = subprocess.run(  # noqa: S603
        (  # noqa: S607
            "git",
            "-c",
            f"safe.directory={repository_root}",
            "rev-parse",
            f"HEAD^:{path}",
        ),
        cwd=repository_root,
        capture_output=True,
        text=True,
    )
    if result.returncode:
        raise RuntimeError(
            "The upstream CI contract requires the parent commit. "
            "Fetch at least two commits before running toolbox validation."
        )
    return result.stdout.strip()


def validate_upstream_ci_contract(
    repository_root: Path = REPOSITORY_ROOT,
    contract: Path = CONTRACT,
    blob_resolver: Callable[[str], str] | None = None,
) -> list[str]:
    document = json.loads(contract.read_text())
    if document.get("schema") != 1:
        return ["unsupported upstream CI contract schema"]

    protected = document.get("protected_parent_blobs")
    if not isinstance(protected, dict) or not protected:
        return ["upstream CI contract has no protected parent blobs"]

    resolve = blob_resolver or (lambda path: _parent_blob(repository_root, path))
    errors = []
    for path, expected in protected.items():
        try:
            actual = resolve(path)
        except RuntimeError as error:
            return [str(error)]
        if actual != expected:
            errors.append(
                f"{path}: parent blob changed from {expected} to {actual}. "
                "Audit the inherited triggers, jobs, axes, selectors, and "
                "postconditions before refreshing the contract."
            )
    return errors


def main() -> int:
    errors = validate_upstream_ci_contract()
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
