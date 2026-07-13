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

import json
from pathlib import Path

import pytest

from .profiles import (
    SUITE_DEFINITIONS,
    SUPPORTED_PYTHON_VERSIONS,
    get_case,
    iter_cases,
    iter_suite_cases,
    make_case,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
RUNTIME_DOCKERFILE = REPOSITORY_ROOT / "etc/docker/test/runtime.Dockerfile"
UPSTREAM_CI_CONTRACT = REPOSITORY_ROOT / ".github/upstream-ci-contract.json"

REQUIRED_CASE_IDS = frozenset(
    f"{suite}-py{python.replace('.', '')}{suffix}"
    for python in SUPPORTED_PYTHON_VERSIONS
    for suite, suffixes in (
        ("unit", ("",)),
        ("client", ("-postgres14",)),
        ("remote-dbs", ("-oracle", "-postgres14")),
        ("multi-vo", ("-postgres14",)),
        ("votest", ("-postgres14-atlas", "-postgres14-belleii")),
        ("integration", ("-postgres14",)),
    )
    for suffix in suffixes
)

INTENTIONALLY_EXCLUDED_CASE_IDS = frozenset({
    "remote-dbs-py39-mysql8",
    "remote-dbs-py310-mysql8",
    "sqlite-py39-sqlite",
    "sqlite-py310-sqlite",
})

UPSTREAM_INTEGRATION_SELECTORS = frozenset({
    "tests/test_rucio_server.py",
    "tests/test_upload.py",
    "tests/test_impl_upload_download.py",
    "tests/test_rse_protocol_gfal2_impl.py",
    "tests/test_rse_protocol_xrootd.py",
    "tests/test_rse_protocol_ssh.py",
    "tests/test_rse_protocol_rsync.py",
    "tests/test_rse_protocol_rclone.py",
    "tests/test_conveyor.py",
    "tests/test_tpc.py",
    "tests/test_reaper.py::test_deletion_with_tokens",
    "tests/test_download.py::test_download_from_archive_on_xrd",
    "tests/test_did_meta_plugins.py::TestDidMetaMongo",
    "tests/test_did_meta_plugins.py::TestDidMetaExternalPostgresJSON",
    "tests/test_did_meta_plugins.py::TestDidMetaElastic",
})


def test_case_registry_covers_current_ci() -> None:
    cases = list(iter_cases())
    case_ids = {case.id for case in cases}

    assert case_ids == REQUIRED_CASE_IDS
    assert INTENTIONALLY_EXCLUDED_CASE_IDS.isdisjoint(case_ids)
    assert len(case_ids) == len(cases)


def test_intentional_case_exclusions_match_ci_contract() -> None:
    contract = json.loads(UPSTREAM_CI_CONTRACT.read_text())

    assert set(contract["intentional_case_exclusions"]) == INTENTIONALLY_EXCLUDED_CASE_IDS
    assert "sqlite" not in SUITE_DEFINITIONS
    assert all("mysql8" not in definition.rdbms for definition in SUITE_DEFINITIONS.values())


def test_regular_case_axes_match_current_ci() -> None:
    actual = {
        (case.suite, case.python, case.rdbms)
        for case in iter_cases("autotest")
    }
    required = {
        (suite, python, rdbms)
        for python in SUPPORTED_PYTHON_VERSIONS
        for suite, rdbms in (
            ("client", "postgres14"),
            ("remote_dbs", "oracle"),
            ("remote_dbs", "postgres14"),
            ("multi_vo", "postgres14"),
        )
    }

    assert required == actual
    assert not any(rdbms in {"mysql8", "sqlite"} for _, _, rdbms in actual)


def test_upstream_test_selectors_are_preserved() -> None:
    assert {"tests/rucio"} <= set(SUITE_DEFINITIONS["unit"].test_paths)
    assert {
        "tests/test_clients.py",
        "tests/test_bin_rucio.py",
        "tests/test_module_import.py",
    } <= set(SUITE_DEFINITIONS["client"].test_paths)
    assert UPSTREAM_INTEGRATION_SELECTORS <= set(
        SUITE_DEFINITIONS["integration"].test_paths
    )


def test_case_ids_include_every_axis() -> None:
    assert get_case("remote-dbs-py310-oracle").rdbms == "oracle"
    assert get_case("votest-py39-postgres14-atlas").policy == "atlas"

    with pytest.raises(ValueError, match="Unknown test case"):
        get_case("remote-dbs")


def test_explicit_axes_build_the_matching_canonical_case() -> None:
    case = make_case("remote_dbs", "3.13", rdbms="postgres14")

    assert case.id == "remote-dbs-py313-postgres14"
    assert case.runtime == "py313"
    assert case.runtime_image_variable == "RUCIO_TEST_IMAGE_PY313"
    assert case.compose_profiles == ("postgres14", "test-dependencies")
    assert case.env_vars == {
        "RDBMS": "postgres14",
        "SUITE": "remote_dbs",
    }
    assert case == get_case("remote-dbs-py313-postgres14")


def test_explicit_python_replaces_only_the_suite_python_axis() -> None:
    cases = tuple(iter_suite_cases("remote_dbs", python="3.13"))

    assert {case.id for case in cases} == {
        "remote-dbs-py313-oracle",
        "remote-dbs-py313-postgres14",
    }


def test_runtime_dockerfile_pins_every_supported_python() -> None:
    dockerfile = RUNTIME_DOCKERFILE.read_text()

    for python_version in SUPPORTED_PYTHON_VERSIONS[1:]:
        version_key = python_version.replace(".", "")
        assert f'ENV PYTHON_{version_key}_VERSION="' in dockerfile
        assert f'ENV PYTHON_{version_key}_SHA256="' in dockerfile
    assert 'ENV BOOST_VERSION="1.81.0"' in dockerfile
    assert '"3.10" "3.11" "3.12" "3.13"' in dockerfile


@pytest.mark.parametrize(
    ("kwargs", "message"),
    (
        ({"python": "3.14", "rdbms": "postgres14"}, "Unsupported Python version"),
        ({"python": "3.13", "rdbms": "mysql8"}, "does not support RDBMS"),
        ({"python": "3.13", "rdbms": "oracle"}, "does not support RDBMS"),
        ({"python": "3.13", "rdbms": "postgres14", "policy": "atlas"}, "does not support policy"),
    ),
)
def test_effective_cases_validate_supported_axes(kwargs: dict, message: str) -> None:
    suite = "client" if kwargs.get("rdbms") == "oracle" else "remote_dbs"

    with pytest.raises(ValueError, match=message):
        make_case(suite, **kwargs)


def test_oracle_uses_a_distinct_amd64_runtime() -> None:
    oracle = get_case("remote-dbs-py310-oracle")
    postgres = get_case("remote-dbs-py310-postgres14")

    assert (oracle.runtime, oracle.runtime_target, oracle.runtime_platform) == (
        "py310-oracle",
        "oracle",
        "linux/amd64",
    )
    assert oracle.runtime_image_variable == "RUCIO_TEST_IMAGE_PY310_ORACLE"
    assert (postgres.runtime, postgres.runtime_target, postgres.runtime_platform) == (
        "py310",
        "final",
        None,
    )


def test_cases_export_the_suite_name() -> None:
    assert get_case("client-py39-postgres14").env_vars["SUITE"] == "client"
    assert get_case("multi-vo-py39-postgres14").env_vars["SUITE"] == "multi_vo"


def test_cases_derive_compose_profiles() -> None:
    assert get_case("client-py39-postgres14").compose_profiles == (
        "postgres14",
    )
    assert get_case("remote-dbs-py39-oracle").compose_profiles == (
        "oracle",
        "test-dependencies",
    )
    assert get_case("integration-py39-postgres14").compose_profiles == (
        "postgres14",
        "test-dependencies",
        "storage",
        "externalmetadata",
        "iam",
    )


def test_database_free_unit_cases_support_xdist() -> None:
    assert get_case("unit-py39").xdist_enabled


def test_every_selector_exists() -> None:
    for definition in SUITE_DEFINITIONS.values():
        for selector in definition.test_paths:
            path = selector.split("::", 1)[0]
            assert Path(path).exists(), (
                f"{definition.name} references missing selector {selector}"
            )
