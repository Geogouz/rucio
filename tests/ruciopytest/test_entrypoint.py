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

import os
import subprocess  # noqa: S404
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
ENTRYPOINT = ROOT / "etc/docker/dev/rucio/entrypoint.sh"


@pytest.mark.parametrize("rdbms", ("oracle", "postgres14"))
def test_entrypoint_configures_every_supported_database(rdbms: str) -> None:
    entrypoint = ENTRYPOINT.read_text()
    config_dir = ROOT / "etc/docker/test/extra"

    assert f'elif [ "$RDBMS" == "{rdbms}" ]' in entrypoint
    assert (config_dir / f"rucio_{rdbms}.cfg").is_file()
    assert (config_dir / f"alembic_{rdbms}.ini").is_file() or rdbms == "postgres14"


@pytest.mark.parametrize("rdbms", ("mysql8", "sqlite"))
def test_entrypoint_rejects_removed_database_cases(rdbms: str) -> None:
    config_dir = ROOT / "etc/docker/test/extra"

    assert f'elif [ "$RDBMS" == "{rdbms}" ]' not in ENTRYPOINT.read_text()
    assert not (config_dir / f"rucio_{rdbms}.cfg").exists()
    assert not (config_dir / f"alembic_{rdbms}.ini").exists()


def test_entrypoint_rejects_unknown_database(tmp_path: Path) -> None:
    environment = dict(os.environ)
    environment.update({
        "RDBMS": "unsupported",
        "RUCIO_HOME": str(tmp_path),
        "RUCIO_SOURCE_DIR": str(ROOT),
    })

    result = subprocess.run(  # noqa: S603
        (ENTRYPOINT, "true"),
        capture_output=True,
        env=environment,
        text=True,
    )

    assert result.returncode == 1
    assert result.stderr == "Unsupported RDBMS: unsupported\n"
