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

import yaml

COMPOSE_DIR = Path(__file__).resolve().parents[2] / "etc/docker/dev"
RUNTIME_DOCKERFILE = COMPOSE_DIR.parent / "test/runtime.Dockerfile"
UNIT_DOCKERFILE = COMPOSE_DIR.parent / "test/unit.Dockerfile"
DEFAULT_CONFIG = COMPOSE_DIR.parent / "test/extra/rucio_default.cfg"
DOCKERIGNORE = COMPOSE_DIR.parents[2] / ".dockerignore"


def test_compose_services_do_not_use_global_container_names() -> None:
    compose = yaml.safe_load((COMPOSE_DIR / "docker-compose.yml").read_text())

    assert all(
        "container_name" not in service
        for service in compose["services"].values()
    )


def test_compose_bind_mounts_allow_isolated_projects() -> None:
    compose_text = (COMPOSE_DIR / "docker-compose.yml").read_text()
    compose = yaml.safe_load(compose_text)

    assert ":Z" not in compose_text
    assert all(
        "z" in volume.rsplit(":", 1)[-1].lower()
        for service in compose["services"].values()
        for volume in service.get("volumes", [])
        if isinstance(volume, str) and volume.startswith(".")
    )


def test_database_services_have_healthchecks() -> None:
    compose = yaml.safe_load((COMPOSE_DIR / "docker-compose.yml").read_text())

    assert all(
        "healthcheck" in compose["services"][service]
        for service in ("ruciodb", "oracle")
    )


def test_supported_databases_use_persistent_storage() -> None:
    compose = yaml.safe_load((COMPOSE_DIR / "docker-compose.yml").read_text())

    assert "vol-ruciodb-data:/var/lib/postgresql/data" in compose["services"]["ruciodb"]["volumes"]
    assert "vol-oracle-data:/opt/oracle/oradata" in compose["services"]["oracle"]["volumes"]
    assert compose["volumes"]["vol-ruciodb-data"]["labels"]["rucio.test.database"] == "postgres14"


def test_dev_and_tests_share_postgres_service() -> None:
    compose = yaml.safe_load((COMPOSE_DIR / "docker-compose.yml").read_text())
    overlay = yaml.safe_load(
        (COMPOSE_DIR / "docker-compose.test.yml").read_text()
    )

    assert "postgres14" not in compose["services"]
    assert overlay["services"]["ruciodb"]["profiles"] == ["postgres14"]


def test_compose_omits_unsupported_databases() -> None:
    compose = yaml.safe_load((COMPOSE_DIR / "docker-compose.yml").read_text())

    assert "mysql8" not in compose["services"]


def test_monitoring_stack_matches_elasticsearch() -> None:
    compose = yaml.safe_load((COMPOSE_DIR / "docker-compose.yml").read_text())
    services = compose["services"]
    version = services["elasticsearch"]["image"].rsplit(":", 1)[1]

    assert services["kibana"]["image"] == (
        f"docker.elastic.co/kibana/kibana:{version}"
    )
    assert "logstash" not in services
    assert not (COMPOSE_DIR / "pipeline.conf").exists()
    assert not (
        COMPOSE_DIR.parents[2] / "tools/monitoring/logstash-pipeline"
    ).exists()

    ports = yaml.safe_load((COMPOSE_DIR / "docker-compose.ports.yml").read_text())
    assert "logstash" not in ports["services"]


def test_default_monitoring_uses_direct_elasticsearch() -> None:
    config = configparser.ConfigParser()
    config.read(DEFAULT_CONFIG)

    assert config.get("hermes", "services_list") == "elastic"
    assert config.get("hermes", "elastic_endpoint") == (
        "http://elasticsearch:9200/rucio-events-dev/_bulk"
    )


def test_test_overlay_consumes_prebuilt_image() -> None:
    compose = yaml.safe_load(
        (COMPOSE_DIR / "docker-compose.test.yml").read_text()
    )
    rucio = compose["services"]["rucio"]

    assert "image" in rucio
    assert "build" not in rucio
    assert rucio["environment"]["RUCIO_HOME"] == "${RUCIO_HOME:-/opt/rucio}"


def test_test_overlay_runs_multiarch_services_natively() -> None:
    compose = yaml.safe_load(
        (COMPOSE_DIR / "docker-compose.test.yml").read_text()
    )

    assert all(
        compose["services"][service]["platform"]
        == "${RUCIO_TEST_NATIVE_PLATFORM:-linux/amd64}"
        for service in (
            "elasticsearch",
            "minio",
            "mongo",
            "mongo-noauth",
        )
    )


def test_auxiliary_services_use_test_dependency_profile() -> None:
    compose = yaml.safe_load(
        (COMPOSE_DIR / "docker-compose.test.yml").read_text()
    )

    assert all(
        compose["services"][service]["profiles"] == ["test-dependencies"]
        for service in (
            "graphite",
            "influxdb",
            "elasticsearch",
            "activemq",
            "web1",
        )
    )


def test_unit_dockerfile_has_parameterized_runtime() -> None:
    dockerfile = UNIT_DOCKERFILE.read_text()

    assert "FROM python:${PYTHON}-slim-bookworm" in dockerfile
    assert "libmagic1" in dockerfile
    assert 'ENTRYPOINT ["python", "-bb", "-m", "pytest"]' in dockerfile


def test_runtime_dockerfile_exposes_source_commands() -> None:
    dockerfile = RUNTIME_DOCKERFILE.read_text()

    assert 'ENV PATH="/rucio_source/bin:${PYTHON_VENV}/bin:${PATH}"' in dockerfile
    assert "VOLUME /opt/rucio" not in dockerfile


def test_runtime_dependency_stage_only_copies_requirements() -> None:
    dockerfile = RUNTIME_DOCKERFILE.read_text()
    dependency_stages, marker, _ = dockerfile.partition("FROM requirements AS final")
    local_copies = [
        line.strip() for line in dependency_stages.splitlines()
        if line.strip().startswith("COPY ")
        and "--from=" not in line
    ]

    assert marker
    assert local_copies == ["COPY requirements /tmp/requirements"]


def test_build_context_omits_local_state() -> None:
    ignored = set(DOCKERIGNORE.read_text().splitlines())

    assert {".git", ".venv", ".test-logs", ".pytest_cache"} <= ignored
