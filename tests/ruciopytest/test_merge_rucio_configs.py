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

from tools.merge_rucio_configs import merge_configs

if TYPE_CHECKING:
    from pathlib import Path


def test_merge_config_can_be_imported_and_called(tmp_path: "Path") -> None:
    first = tmp_path / "first.cfg"
    second = tmp_path / "second.cfg"
    destination = tmp_path / "merged.cfg"
    first.write_text("[section]\nfirst = one\nshared = old\n")
    second.write_text("[section]\nsecond = two\nshared = new\n")

    merge_configs([first, second], destination, use_env=False)

    actual = configparser.ConfigParser()
    actual.read(destination)
    assert dict(actual["section"]) == {
        "first": "one",
        "second": "two",
        "shared": "new",
    }


def test_merge_config_applies_environment_last(
    tmp_path: "Path",
    monkeypatch,
) -> None:
    source = tmp_path / "source.cfg"
    destination = tmp_path / "merged.cfg"
    source.write_text("[section]\nvalue = file\n")
    monkeypatch.setenv("RUCIO_CFG_SECTION_VALUE", "environment")

    merge_configs([source], destination, use_env=True)

    actual = configparser.ConfigParser()
    actual.read(destination)
    assert actual["section"]["value"] == "environment"
