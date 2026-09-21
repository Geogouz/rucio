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

import shutil
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence


@contextmanager
def temporary_docker_context(root: Path, inputs: "Sequence[Path]") -> "Iterator[Path]":
    root = root.resolve()
    with TemporaryDirectory(prefix="rucio-docker-context-") as temporary:
        context = Path(temporary)
        for configured in inputs:
            relative = Path(configured)
            if relative.is_absolute() or relative == Path(".") or ".." in relative.parts:
                raise ValueError(f"Docker context input must be a relative file: {configured}")

            source = root / relative
            try:
                resolved = source.resolve(strict=True)
                resolved.relative_to(root)
            except (FileNotFoundError, ValueError) as error:
                raise RuntimeError(f"Docker context input is missing or outside the repository: {relative}") from error
            if resolved != source.absolute() or not source.is_file():
                raise RuntimeError(f"Docker context input must be a regular repository file: {relative}")

            destination = context / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, destination)
            shutil.copymode(source, destination)
        yield context
