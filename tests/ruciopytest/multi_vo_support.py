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

import shutil
from typing import TYPE_CHECKING

from tools.merge_rucio_configs import merge_configs

if TYPE_CHECKING:
    from pathlib import Path


def generate_multi_vo_configs(
    repo_root: "Path",
    destination_root: "Path",
) -> dict[str, "Path"]:
    config_dir = repo_root / "etc/docker/test/extra"
    common = config_dir / "rucio_autotests_common.cfg"
    alembic = config_dir / "alembic_default.ini"
    generated = {}

    for vo, source_name in (
        ("tst", "rucio_multi_vo_tst_postgres14.cfg"),
        ("ts2", "rucio_multi_vo_ts2_postgres14.cfg"),
    ):
        destination = destination_root / vo / "etc"
        destination.mkdir(parents=True, exist_ok=True)
        rucio_cfg = destination / "rucio.cfg"
        merge_configs(
            [common, config_dir / source_name],
            rucio_cfg,
            use_env=True,
        )
        shutil.copyfile(alembic, destination / "alembic.ini")
        generated[vo] = rucio_cfg

    return generated
