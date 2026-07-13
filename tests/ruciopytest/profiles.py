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

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator
    from typing import Optional


_XDIST_RDBMS = frozenset({"postgres14"})
SUPPORTED_PYTHON_VERSIONS = ("3.9", "3.10", "3.11", "3.12", "3.13")


@dataclass(frozen=True)
class SuiteDefinition:
    name: str
    group: str
    python_versions: tuple[str, ...]
    rdbms: tuple[str, ...] = ()
    test_paths: tuple[str, ...] = ("tests/",)
    exclude_paths: tuple[str, ...] = ("tests/ruciopytest/*",)
    compose_profiles: tuple[str, ...] = ()
    policies: tuple[str, ...] = ()
    env_vars: dict[str, str] = field(default_factory=dict)
    xdist_enabled: bool = True


@dataclass(frozen=True)
class TestCase:
    suite: str
    group: str
    python: str
    rdbms: str = ""
    policy: str = ""
    test_paths: tuple[str, ...] = ("tests/",)
    exclude_paths: tuple[str, ...] = ()
    compose_profiles: tuple[str, ...] = ()
    env_vars: dict[str, str] = field(default_factory=dict)
    xdist_enabled: bool = False

    @property
    def id(self) -> str:
        parts = [self.suite.replace("_", "-"), f"py{self.python.replace('.', '')}"]
        if self.rdbms:
            parts.append(self.rdbms)
        if self.policy:
            parts.append(self.policy)
        return "-".join(parts)

    @property
    def runtime(self) -> str:
        runtime = f"py{self.python.replace('.', '')}"
        return f"{runtime}-oracle" if self.rdbms == "oracle" else runtime

    @property
    def runtime_image_variable(self) -> str:
        return f"RUCIO_TEST_IMAGE_{self.runtime.replace('-', '_').upper()}"

    @property
    def runtime_target(self) -> str:
        return "oracle" if self.rdbms == "oracle" else "final"

    @property
    def runtime_platform(self) -> "Optional[str]":
        return "linux/amd64" if self.rdbms == "oracle" else None


SUITE_DEFINITIONS: dict[str, SuiteDefinition] = {
    "unit": SuiteDefinition(
        name="unit",
        group="unit",
        python_versions=SUPPORTED_PYTHON_VERSIONS,
        test_paths=("tests/rucio", "tests/ruciopytest"),
        exclude_paths=(),
    ),
    "client": SuiteDefinition(
        name="client",
        group="autotest",
        python_versions=SUPPORTED_PYTHON_VERSIONS,
        rdbms=("postgres14",),
        test_paths=(
            "tests/test_clients.py",
            "tests/test_bin_rucio.py",
            "tests/test_module_import.py",
        ),
        exclude_paths=(),
    ),
    "remote_dbs": SuiteDefinition(
        name="remote_dbs",
        group="autotest",
        python_versions=SUPPORTED_PYTHON_VERSIONS,
        rdbms=("oracle", "postgres14"),
        compose_profiles=("test-dependencies",),
    ),
    "multi_vo": SuiteDefinition(
        name="multi_vo",
        group="autotest",
        python_versions=SUPPORTED_PYTHON_VERSIONS,
        rdbms=("postgres14",),
        compose_profiles=("test-dependencies",),
        env_vars={"RUCIO_HOME": "/opt/rucio/etc/multi_vo/tst"},
    ),
    "votest": SuiteDefinition(
        name="votest",
        group="votest",
        python_versions=SUPPORTED_PYTHON_VERSIONS,
        rdbms=("postgres14",),
        compose_profiles=("test-dependencies",),
        policies=("atlas", "belleii"),
    ),
    "integration": SuiteDefinition(
        name="integration",
        group="integration",
        python_versions=SUPPORTED_PYTHON_VERSIONS,
        rdbms=("postgres14",),
        compose_profiles=(
            "test-dependencies",
            "storage",
            "externalmetadata",
            "iam",
        ),
        test_paths=(
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
        ),
        exclude_paths=(),
        xdist_enabled=False,
    ),
}


def make_case(
    suite: str,
    python: str,
    *,
    rdbms: str = "",
    policy: str = "",
) -> TestCase:
    try:
        definition = SUITE_DEFINITIONS[suite]
    except KeyError as error:
        raise ValueError(f"Unknown test suite {suite!r}") from error
    if python not in SUPPORTED_PYTHON_VERSIONS:
        supported = ", ".join(SUPPORTED_PYTHON_VERSIONS)
        raise ValueError(f"Unsupported Python version {python!r}; choose one of: {supported}")
    if rdbms not in (definition.rdbms or ("",)):
        raise ValueError(f"Suite {suite!r} does not support RDBMS {rdbms!r}")
    if policy not in (definition.policies or ("",)):
        raise ValueError(f"Suite {suite!r} does not support policy {policy!r}")

    profiles = definition.compose_profiles
    if rdbms:
        profiles = (rdbms, *profiles)
    env_vars = dict(definition.env_vars)
    if rdbms:
        env_vars["RDBMS"] = rdbms
    if policy:
        env_vars["POLICY"] = policy
    env_vars["SUITE"] = definition.name
    return TestCase(
        suite=definition.name,
        group=definition.group,
        python=python,
        rdbms=rdbms,
        policy=policy,
        test_paths=definition.test_paths,
        exclude_paths=definition.exclude_paths,
        compose_profiles=profiles,
        env_vars=env_vars,
        xdist_enabled=(
            definition.xdist_enabled
            and (not rdbms or rdbms in _XDIST_RDBMS)
        ),
    )


def iter_suite_cases(
    suite: str,
    *,
    python: "Optional[str]" = None,
    rdbms: "Optional[str]" = None,
    policy: "Optional[str]" = None,
) -> "Iterator[TestCase]":
    try:
        definition = SUITE_DEFINITIONS[suite]
    except KeyError as error:
        raise ValueError(f"Unknown test suite {suite!r}") from error
    python_versions = (python,) if python is not None else definition.python_versions
    databases = (rdbms,) if rdbms is not None else definition.rdbms or ("",)
    policies = (policy,) if policy is not None else definition.policies or ("",)
    for python_version in python_versions:
        for database in databases:
            for case_policy in policies:
                yield make_case(
                    suite,
                    python_version,
                    rdbms=database,
                    policy=case_policy,
                )


def iter_cases(group: "Optional[str]" = None) -> "Iterator[TestCase]":
    for definition in SUITE_DEFINITIONS.values():
        if group is not None and definition.group != group:
            continue
        yield from iter_suite_cases(definition.name)


def get_case(case_id: str) -> TestCase:
    for case in iter_cases():
        if case.id == case_id:
            return case
    raise ValueError(f"Unknown test case {case_id!r}")
