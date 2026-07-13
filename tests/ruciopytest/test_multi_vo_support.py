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
import os
from pathlib import Path

from tests.ruciopytest.multi_vo_support import generate_multi_vo_configs

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_generate_multi_vo_configs(tmp_path: Path, monkeypatch) -> None:
    for key in list(os.environ):
        if key.startswith("RUCIO_CFG_"):
            monkeypatch.delenv(key, raising=False)

    generated = generate_multi_vo_configs(REPO_ROOT, tmp_path)

    assert set(generated) == {"tst", "ts2"}
    for vo, expected_long_vo in (("tst", "testvo1"), ("ts2", "testvo2")):
        config = configparser.ConfigParser()
        config.read(generated[vo])
        assert config["client"]["vo"] == expected_long_vo
        assert config["common"].getboolean("multi_vo")
        assert "@ruciodb/" in config["database"]["default"]
        assert generated[vo].with_name("alembic.ini").is_file()
