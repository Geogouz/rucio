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

COMPOSE_DIR = Path(__file__).resolve().parents[2] / "etc/docker/dev"
RUNTIME_DOCKERFILE = COMPOSE_DIR.parent / "test/runtime.Dockerfile"


def test_compose_services_do_not_use_global_container_names() -> None:
    compose = yaml.safe_load((COMPOSE_DIR / "docker-compose.yml").read_text())

    assert all(
        "container_name" not in service
        for service in compose["services"].values()
    )


def test_database_services_have_healthchecks() -> None:
    compose = yaml.safe_load((COMPOSE_DIR / "docker-compose.yml").read_text())

    assert all(
        "healthcheck" in compose["services"][service]
        for service in ("postgres14", "mysql8", "oracle")
    )


def test_all_databases_use_persistent_storage() -> None:
    compose = yaml.safe_load((COMPOSE_DIR / "docker-compose.yml").read_text())
    overlay = yaml.safe_load((COMPOSE_DIR / "docker-compose.test.yml").read_text())

    assert "vol-postgres14-data:/var/lib/postgresql/data" in compose["services"]["postgres14"]["volumes"]
    assert "vol-mysql8-mysql:/var/lib/mysql" in compose["services"]["mysql8"]["volumes"]
    assert "vol-oracle-data:/opt/oracle/oradata" in compose["services"]["oracle"]["volumes"]
    assert "vol-test-tmp:/tmp" in overlay["services"]["rucio"]["volumes"]


def test_test_overlay_consumes_prebuilt_image() -> None:
    compose = yaml.safe_load(
        (COMPOSE_DIR / "docker-compose.test.yml").read_text()
    )
    rucio = compose["services"]["rucio"]

    assert "image" in rucio
    assert "build" not in rucio
    assert rucio["environment"]["RUCIO_HOME"] == "${RUCIO_HOME:-/opt/rucio}"


def test_runtime_dockerfile_has_parameterized_unit_target() -> None:
    dockerfile = RUNTIME_DOCKERFILE.read_text()

    assert "FROM python:${PYTHON}-slim-bookworm AS unit" in dockerfile
    assert 'ENTRYPOINT ["python", "-bb", "-m", "pytest"]' in dockerfile


def test_runtime_dockerfile_exposes_source_commands() -> None:
    dockerfile = RUNTIME_DOCKERFILE.read_text()

    assert 'ENV PATH="/rucio_source/bin:${PYTHON_VENV}/bin:${PATH}"' in dockerfile
