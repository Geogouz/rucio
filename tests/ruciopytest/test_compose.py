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
AUTOTEST_CONFIG = COMPOSE_DIR.parent / "test/extra/rucio_autotests_common.cfg"
HTTPD_RUCIO_CONFIG = COMPOSE_DIR.parent / "test/extra/rucio.conf"
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


def test_removed_database_cases_are_not_exposed() -> None:
    compose = yaml.safe_load((COMPOSE_DIR / "docker-compose.yml").read_text())

    assert "mysql8" not in compose["services"]
    assert "vol-mysql8-data" not in compose["volumes"]


def test_compose_omits_legacy_services_and_test_only_ports() -> None:
    compose = yaml.safe_load((COMPOSE_DIR / "docker-compose.yml").read_text())
    services = compose["services"]
    assert {"httpd", "kibana", "grafana"}.isdisjoint(services)
    assert all(
        services[service]["profiles"] == ["test-dependencies"]
        for service in ("graphite", "influxdb", "elasticsearch")
    )

    ports = yaml.safe_load((COMPOSE_DIR / "docker-compose.ports.yml").read_text())
    assert {
        "httpd",
        "graphite",
        "influxdb",
        "elasticsearch",
        "oracle",
        "kibana",
        "grafana",
        "rucio",
        "workspace",
    }.isdisjoint(ports["services"])


def test_dev_compose_separates_api_and_workspace_processes() -> None:
    compose = yaml.safe_load((COMPOSE_DIR / "docker-compose.yml").read_text())
    services = compose["services"]

    assert compose["x-rucio-environment"]["PYTHONPYCACHEPREFIX"] == "/tmp/rucio-pycache"
    assert services["rucio"]["command"] == ["httpd", "-D", "FOREGROUND"]
    assert services["rucio"]["healthcheck"]["test"][-1] == "https://localhost/ping"
    assert services["workspace"]["command"] == ["sleep", "infinity"]
    assert services["workspace"]["profiles"] == ["workspace"]
    assert services["workspace"]["environment"]["PYTHONPATH"] == ""
    assert services["workspace"]["environment"]["RUCIO_CFG_CLIENT_RUCIO_HOST"] == "https://rucio:443"
    assert services["workspace"]["environment"]["RUCIO_CFG_CLIENT_AUTH_HOST"] == "https://rucio:443"


def test_dev_execution_overlays_only_modify_workspace() -> None:
    for filename in (
        "docker-compose.api.yml",
        "docker-compose.debug.yml",
        "docker-compose.instrumentation.yml",
        "docker-compose.observe.yml",
    ):
        overlay = yaml.safe_load((COMPOSE_DIR / filename).read_text())
        assert set(overlay["services"]) - {"otel-lgtm"} == {"workspace"}


def test_debug_and_api_ports_are_fixed_and_loopback_only() -> None:
    expected = {
        "docker-compose.api.yml": ("workspace", "127.0.0.1:8080:8080"),
        "docker-compose.debug.yml": ("workspace", "127.0.0.1:5678:5678"),
    }

    for filename, (service, port) in expected.items():
        overlay = yaml.safe_load((COMPOSE_DIR / filename).read_text())
        assert overlay["services"][service]["ports"] == [port]


def test_canonical_debug_overlays_only_modify_rucio() -> None:
    for filename in (
        "docker-compose.test-debug.yml",
        "docker-compose.test-instrumentation.yml",
    ):
        overlay = yaml.safe_load((COMPOSE_DIR / filename).read_text())
        assert set(overlay["services"]) == {"rucio"}


def test_canonical_observe_overlay_instruments_httpd_and_joins_backend() -> None:
    overlay = yaml.safe_load(
        (COMPOSE_DIR / "docker-compose.test-observe.yml").read_text()
    )
    rucio = overlay["services"]["rucio"]

    assert set(overlay["services"]) == {"rucio"}
    assert rucio["command"][:4] == [
        "opentelemetry-instrument",
        "--service_name",
        "rucio-test-server",
        "httpd",
    ]
    assert rucio["environment"]["OTEL_EXPORTER_OTLP_ENDPOINT"] == "http://otel-lgtm:4318"
    assert rucio["networks"] == ["default", "observe"]
    assert overlay["networks"]["observe"]["external"] is True


def test_default_hermes_uses_direct_elasticsearch() -> None:
    config = configparser.ConfigParser()
    config.read(DEFAULT_CONFIG)

    assert config.get("hermes", "services_list") == "elastic"
    assert config.get("hermes", "elastic_endpoint") == (
        "http://elasticsearch:9200/rucio-events-dev/_bulk"
    )


def test_compose_only_consumes_prebuilt_runtime_images() -> None:
    base = yaml.safe_load((COMPOSE_DIR / "docker-compose.yml").read_text())
    compose = yaml.safe_load(
        (COMPOSE_DIR / "docker-compose.test.yml").read_text()
    )
    rucio = compose["services"]["rucio"]

    assert "build" not in base["x-rucio-runtime"]
    assert "image" in rucio
    assert "build" not in rucio
    assert rucio["environment"]["RUCIO_HOME"] == "${RUCIO_HOME:-/opt/rucio}"
    assert "workspace" not in compose["services"]


def test_test_overlay_runs_multiarch_services_natively() -> None:
    base = yaml.safe_load((COMPOSE_DIR / "docker-compose.yml").read_text())
    overlay = yaml.safe_load(
        (COMPOSE_DIR / "docker-compose.test.yml").read_text()
    )

    assert overlay["services"]["rucio"]["platform"] == "${RUCIO_TEST_IMAGE_PLATFORM:-linux/amd64}"
    assert all(
        overlay["services"][service]["platform"]
        == "${RUCIO_TEST_NATIVE_PLATFORM:-linux/amd64}"
        for service in (
            "ruciodb",
            "graphite",
            "influxdb",
            "elasticsearch",
            "activemq",
            "minio",
            "mongo",
            "mongo-noauth",
        )
    )
    assert all(
        "platform" not in base["services"][service]
        for service in ("xrd1", "xrd2", "xrd3", "xrd4", "xrd5", "web1", "ssh1")
    )
    assert all(base["services"][service]["platform"] == "linux/amd64" for service in ("fts", "oracle"))


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


def test_runtime_configuration_uses_the_canonical_source_mount() -> None:
    rucio_config = HTTPD_RUCIO_CONFIG.read_text()
    assert "/rucio_source/lib/rucio/web/rest/main.py" in rucio_config
    assert "python-home=/opt/venv" in rucio_config
    for config_path in (DEFAULT_CONFIG, AUTOTEST_CONFIG):
        config = configparser.ConfigParser()
        config.read(config_path)
        assert config.get("injector", "file") == "/rucio_source/tools/test.file.1000"


def test_runtime_dependency_stage_only_copies_requirements() -> None:
    dockerfile = RUNTIME_DOCKERFILE.read_text()
    dependency_stages, marker, _ = dockerfile.partition("FROM requirements AS configured")
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
