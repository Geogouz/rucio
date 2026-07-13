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

import yaml


def test_compose_services_do_not_use_global_container_names() -> None:
    compose_path = (
        Path(__file__).resolve().parents[2]
        / "etc/docker/dev/docker-compose.yml"
    )
    compose = yaml.safe_load(compose_path.read_text())

    assert all(
        "container_name" not in service
        for service in compose["services"].values()
    )
