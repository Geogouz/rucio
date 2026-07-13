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
from pathlib import Path

from tools.merge_rucio_configs import merge_configs

ROOT = Path(__file__).resolve().parents[2]


def test_dev_daemon_config_overrides_test_defaults(monkeypatch, tmp_path: Path) -> None:
    common = ROOT / "etc/docker/test/extra/rucio_autotests_common.cfg"
    postgres = ROOT / "etc/docker/test/extra/rucio_postgres14.cfg"
    development = ROOT / "etc/docker/dev/rucio/rucio_dev.cfg"
    output = tmp_path / "rucio.cfg"
    monkeypatch.setenv("RUCIO_CFG_MESSAGING_HERMES_DESTINATION", "/queue/environment-wins")

    merge_configs([common, postgres, development], output, use_env=True)

    config = configparser.ConfigParser()
    config.read(output)
    assert config.get("hermes", "services_list") == "activemq"
    assert config.get("messaging-hermes", "username") == "hermes"
    assert config.getint("messaging-hermes", "nonssl_port") == 61613
    assert config.get("messaging-hermes", "destination") == "/queue/environment-wins"
    assert config.get("tracer-kronos", "username") == "kronos"
    assert config.getint("tracer-kronos", "port") == 61613
    assert config.get("trace", "username") == "trace"
    assert config.getint("trace", "port") == 61613
    assert config.getboolean("trace", "use_ssl") is False
    assert config.get("messaging-cache", "username") == "cache"
    assert config.getint("messaging-cache", "port") == 61613
    assert config.getboolean("messaging-cache", "use_ssl") is False


def test_canonical_config_leaves_hermes_services_test_controlled(tmp_path: Path) -> None:
    common = ROOT / "etc/docker/test/extra/rucio_autotests_common.cfg"
    postgres = ROOT / "etc/docker/test/extra/rucio_postgres14.cfg"
    output = tmp_path / "rucio.cfg"

    merge_configs([common, postgres], output, use_env=True)

    config = configparser.ConfigParser()
    config.read(output)
    assert not config.has_option("hermes", "services_list")


def test_dev_daemon_credentials_exist_in_activemq_users() -> None:
    config = configparser.ConfigParser()
    config.read(ROOT / "etc/docker/dev/rucio/rucio_dev.cfg")
    users = dict(
        line.split("=", maxsplit=1)
        for line in (ROOT / "etc/docker/dev/activemq/users.properties").read_text().splitlines()
        if line
    )

    for section in ("messaging-hermes", "tracer-kronos", "trace", "messaging-cache"):
        username = config.get(section, "username")
        assert users[username] == config.get(section, "password")
