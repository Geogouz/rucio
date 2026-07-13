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

from __future__ import annotations

import importlib.util
import json
import os
import subprocess  # noqa: S404
import sys
import xml.etree.ElementTree as ElementTree  # noqa: S405
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from tests.ruciopytest import performance_outcomes
from tests.ruciopytest.container_manager import (
    REPORT_IMAGE_MANIFEST_ENV,
    REPORT_IMAGE_MANIFEST_PREFIX,
    REPORT_SOURCE_GID_ENV,
    REPORT_SOURCE_UID_ENV,
    ContainerManager,
)
from tests.ruciopytest.profiles import get_case

if TYPE_CHECKING:
    from types import ModuleType


def _load_reporter() -> ModuleType:
    path = Path(__file__).resolve().parents[2] / "tools/test/run_test_performance_report.py"
    spec = importlib.util.spec_from_file_location("rucio_test_performance_report", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


reporter = _load_reporter()


def _runtime_environment() -> object:
    return reporter.RuntimeEnvironment(
        host_system="Linux",
        host_release="6.8",
        host_machine="aarch64",
        host_cpu_count=8,
        image="runtime:test",
        image_build_fingerprint="f" * 64,
        image_id="sha256:" + "1" * 64,
        image_os="linux",
        image_architecture="arm64",
        runtime_system="Linux",
        runtime_release="6.8",
        runtime_machine="aarch64",
        runtime_python="3.10.4",
        runtime_cpu_count=8,
        runtime_affinity_count=6,
        xdist_version="3.5.0",
        dependency_images=(
            reporter.DependencyImage(
                service="ruciodb",
                reference="docker.io/postgres:14",
                image_id="sha256:" + "2" * 64,
                repo_digests=("docker.io/library/postgres@sha256:" + "3" * 64,),
            ),
        ),
        docker_server_version="27.5.1",
        docker_server_os="Docker Desktop",
        docker_server_kernel="6.10.14-linuxkit",
        docker_server_architecture="arm64",
        docker_server_cpu_count=8,
        docker_server_memory_bytes=8 * 1024**3,
        runtime_cpu_quota=6.0,
        runtime_memory_limit_bytes=6 * 1024**3,
        runtime_cpu_limit_source="cgroup-v2",
        runtime_memory_limit_source="cgroup-v2",
    )


def _runtime_probe() -> object:
    return reporter.RuntimeProbe(
        system="Linux",
        release="6.8",
        machine="aarch64",
        python="3.10.4",
        cpu_count=8,
        affinity_count=6,
        cpu_quota=6.0,
        cpu_limit_source="cgroup-v2",
        memory_limit_bytes=6 * 1024**3,
        memory_limit_source="cgroup-v2",
        xdist_version="3.5.0",
    )


def _case_identity() -> object:
    case = get_case(reporter.CASE_NAME)
    return reporter.ResolvedCase(
        name=case.id,
        suite=case.suite,
        group=case.group,
        python=case.python,
        rdbms=case.rdbms,
        policy=case.policy,
        test_paths=case.test_paths,
        exclude_paths=case.exclude_paths,
        compose_profiles=case.compose_profiles,
        environment=tuple(sorted(case.env_vars.items())),
        xdist_enabled=case.xdist_enabled,
        runtime=case.runtime,
        runtime_target=case.runtime_target,
        runtime_platform=case.runtime_platform,
    )


def _run_result(*, selectors: tuple[str, ...] = (), returncode: int = 0) -> object:
    project = reporter.canonical_project_name(reporter.CASE_NAME, "0123abcd")
    environment = _runtime_environment()
    return reporter.RunResult(
        returncode=returncode,
        wall_time=4.0,
        command="python -m pytest <selection>",
        configured_workers=2,
        observed_workers=2,
        selectors=selectors,
        network_name=f"{project}-network",
        compose_project=project,
        cleanup_complete=True,
        removed_residual_volume_count=3,
        preexisting_rucio_containers=(),
        runtime_image_id=environment.image_id,
        dependency_images=environment.dependency_images,
        runtime_probe=_runtime_probe(),
        case_identity=_case_identity(),
        harness_fingerprint="4" * 64,
        test_manifest_fingerprint="5" * 64,
    )


def test_parse_arguments_and_normalize_selectors() -> None:
    args = reporter.parse_arguments([
        "--workers",
        "2",
        "--output-root",
        ".autotest/custom",
        "--",
        "tests/test_ping.py::test_rucio_ping",
        "-k",
        "ping",
    ])

    assert args.workers == 2
    assert args.output_root == Path(".autotest/custom")
    assert reporter.normalize_test_selectors(args.test_selectors) == (
        "tests/test_ping.py::test_rucio_ping",
        "-k",
        "ping",
    )


@pytest.mark.parametrize(
    "arguments",
    (
        ("--case=client-py310-postgres14",),
        ("--suite", "remote_dbs"),
        ("--case-workers=2",),
        ("--junitxml", "other.xml"),
        ("--junit-xml=other.xml",),
        ("--container-env=RDBMS=oracle",),
        ("--xdist-workers=1",),
        ("--numprocesses", "1"),
        ("--dist=loadscope",),
        ("--tx", "popen"),
        ("-d",),
        ("-n1",),
        ("--rootdir", "/tmp"),
        ("-c", "other.toml"),
        ("--config-file=other.toml",),
        ("--policy=atlas",),
        ("--keep-db",),
        ("--dry-run",),
        ("--dry-run-json",),
        ("--list-cases",),
    ),
)
def test_reporter_owned_pytest_options_are_rejected(arguments: tuple[str, ...]) -> None:
    with pytest.raises(ValueError, match="Reporter-owned pytest option"):
        reporter.normalize_test_selectors(arguments)


def test_ordinary_pytest_selection_options_remain_supported() -> None:
    assert reporter.normalize_test_selectors(("--", "tests/test_ping.py", "-k", "ping", "--maxfail=1")) == (
        "tests/test_ping.py",
        "-k",
        "ping",
        "--maxfail=1",
    )
    assert reporter.normalize_test_selectors(("--", "--", "--case=literal-path")) == ("--", "--case=literal-path")


def test_project_and_command_match_canonical_test_adapter(tmp_path: Path) -> None:
    nonce = "0123abcd"
    project = reporter.canonical_project_name(reporter.CASE_NAME, nonce)
    workspace = tmp_path / "workspace"
    xml_path = workspace / ".autotest/output/junit.xml"
    command = reporter.canonical_test_command(
        workspace,
        xml_path,
        ("tests/test_ping.py::test_rucio_ping", "-k", "ping"),
        3,
        Path("/control/bin/python"),
    )

    assert project == ContainerManager.make_project_name(
        reporter.CASE_NAME,
        workspace,
        reusable=False,
        nonce=nonce,
    )
    assert command == [
        "/control/bin/python",
        "-m",
        "pytest",
        "--case=remote-dbs-py310-postgres14",
        "--xdist-workers=3",
        "--junitxml=/rucio_source/.autotest/output/junit.xml",
        "-p",
        "tests.ruciopytest.performance_outcomes",
        "tests/test_ping.py::test_rucio_ping",
        "-k",
        "ping",
    ]
    with pytest.raises(ValueError, match="JUnit output must be inside"):
        reporter.canonical_test_command(
            workspace,
            tmp_path / "outside.xml",
            (),
            3,
            Path("/control/bin/python"),
        )

    compose_environment = reporter.canonical_compose_environment(
        {"SAFE_VALUE": "kept"},
        runtime_image="runtime:test",
        runtime_platform="linux/arm64",
        nonce=nonce,
    )
    manager = ContainerManager(
        get_case(reporter.CASE_NAME),
        workspace,
        image="runtime:test",
        environ=reporter.canonical_test_environment(
            {"SAFE_VALUE": "kept"},
            runtime_image="runtime:test",
            runtime_platform="linux/arm64",
            nonce=nonce,
        ),
    )
    manager.environment["RUCIO_TEST_IMAGE_PLATFORM"] = "linux/arm64"
    expected_compose_command = manager.compose_command("config", "--format", "json")
    expected_compose_command[0] = "/usr/bin/docker"

    assert reporter.REPORT_IMAGE_MANIFEST_ENV == REPORT_IMAGE_MANIFEST_ENV
    assert reporter.IMAGE_MANIFEST_PREFIX == REPORT_IMAGE_MANIFEST_PREFIX
    assert reporter.REPORT_SOURCE_UID_ENV == REPORT_SOURCE_UID_ENV
    assert reporter.REPORT_SOURCE_GID_ENV == REPORT_SOURCE_GID_ENV
    assert compose_environment == manager.environment
    assert reporter.canonical_compose_command(
        "/usr/bin/docker",
        workspace,
        project,
        "config",
        "--format",
        "json",
    ) == expected_compose_command


def test_canonical_test_environment_isolated_from_host_controls() -> None:
    environment = reporter.canonical_test_environment(
        {
            "COMPOSE_PROJECT_NAME": "host-project",
            "POLICY": "atlas",
            "PYTEST_ADDOPTS": "-x",
            "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
            "PYTEST_PLUGINS": "host_plugin",
            "PYTEST_XDIST_AUTO_NUM_WORKERS": "64",
            "RUCIO_CFG_DATABASE_DEFAULT": "host-secret",
            "RUCIO_DATABASE_CONTAINER_NAME": "host-database",
            "RUCIO_DEV_PROFILE": "memory",
            "RUCIO_KEEP_TEST_DB": "1",
            "RUCIO_MULTI_VO_LEG": "tst",
            "RUCIO_TEST_IMAGE": "host-runtime",
            "RUCIO_TEST_UNRELATED": "host-control",
            "RUCIO_PYTEST_INNER": "1",
            "RUCIO_SKIP_TEST_SETUP": "1",
            "SAFE_VALUE": "kept",
        },
        runtime_image="runtime:test",
        runtime_platform="linux/arm64",
        nonce="0123abcd",
    )

    assert environment.pop(reporter.REPORT_IMAGE_MANIFEST_ENV) == "1"
    assert environment.pop(reporter.REPORT_SOURCE_UID_ENV).isdecimal()
    assert environment.pop(reporter.REPORT_SOURCE_GID_ENV).isdecimal()
    assert environment == {
        "GITHUB_ACTIONS": "true",
        "RUCIO_DEV_PLATFORM": "linux/arm64",
        "RUCIO_TEST_IMAGE": "runtime:test",
        "RUCIO_TEST_NATIVE_PLATFORM": "linux/arm64",
        "RUCIO_TEST_PROJECT_NONCE": "0123abcd",
        "SAFE_VALUE": "kept",
    }


def test_report_paths_are_private_and_file_writes_reject_symlinks(tmp_path: Path) -> None:
    run_directory = reporter.create_run_directory(tmp_path / "reports", "test-case")
    output = run_directory / "timings.json"
    reporter.write_json(
        (reporter.TestTiming(classname="tests.test_ping", name="ping", duration=1.0),),
        output,
    )

    assert (tmp_path / "reports").stat().st_mode & 0o777 == 0o700
    assert run_directory.stat().st_mode & 0o777 == 0o700
    assert output.stat().st_mode & 0o777 == 0o600

    target = tmp_path / "target"
    target.write_text("unchanged")
    linked_output = run_directory / "metadata.json"
    linked_output.symlink_to(target)
    with pytest.raises(SystemExit, match="safely open private report file"):
        reporter.write_private_text(linked_output, "replaced")
    assert target.read_text() == "unchanged"

    hardlinked_output = run_directory / "hardlinked.json"
    os.link(target, hardlinked_output)
    with pytest.raises(SystemExit, match="multiply linked"):
        reporter.write_private_text(hardlinked_output, "replaced")
    assert target.read_text() == "unchanged"

    junit = run_directory / "junit.xml"
    reporter.write_private_text(junit, "")
    owner = junit.stat().st_uid
    junit.write_text("<testsuites />")
    copied = run_directory / "copied-junit.xml"
    reporter.copy_private_file(junit, copied)
    assert junit.stat().st_uid == owner
    assert copied.read_text() == "<testsuites />"


def test_repository_path_restricts_reports_to_owned_tree(tmp_path: Path, monkeypatch) -> None:
    tmp_path.chmod(0o755)
    (tmp_path / "tests").mkdir()
    monkeypatch.setattr(reporter, "ROOT", tmp_path)

    with pytest.raises(SystemExit, match="owned report root"):
        reporter.repository_path(Path("."))
    assert tmp_path.stat().st_mode & 0o777 == 0o755

    output_root = reporter.repository_path(Path(".autotest/custom"))
    reporter.create_run_directory(output_root, "case")
    assert output_root.stat().st_mode & 0o777 == 0o700
    assert tmp_path.stat().st_mode & 0o777 == 0o755

    (tmp_path / ".autotest/source-link").symlink_to(tmp_path / "tests", target_is_directory=True)
    with pytest.raises(SystemExit, match="owned report root"):
        reporter.repository_path(Path(".autotest/source-link"))


def test_exclusive_lock_rejects_hardlinks(tmp_path: Path, monkeypatch) -> None:
    target = tmp_path / "target"
    target.write_text("unchanged")
    lock = tmp_path / f"rucio-test-performance-report-{os.getuid()}.lock"
    os.link(target, lock)
    monkeypatch.setattr(reporter.tempfile, "gettempdir", lambda: str(tmp_path))

    with pytest.raises(SystemExit, match="multiply linked"):
        with reporter.exclusive_run_lock():
            pytest.fail("hardlinked lock was accepted")
    assert target.read_text() == "unchanged"


def test_run_image_manifest_uses_exact_container_image_ids() -> None:
    payload = {
        "runtime": {
            "service": "rucio",
            "reference": "runtime:test",
            "image_id": "sha256:" + "0" * 64,
            "repo_digests": [],
        },
        "runtime_environment": {
            "system": "Linux",
            "release": "6.8",
            "machine": "aarch64",
            "python": "3.10.4",
            "cpu_count": 8,
            "affinity_count": 6,
            "cpu_quota": 6.0,
            "cpu_limit_source": "cgroup-v2",
            "memory_limit_bytes": 6 * 1024**3,
            "memory_limit_source": "cgroup-v2",
            "xdist_version": "3.5.0",
        },
        "dependencies": [
            {
                "service": "web1",
                "reference": "docker.io/rucio/test-webdav:latest",
                "image_id": "sha256:" + "3" * 64,
                "repo_digests": ["docker.io/rucio/test-webdav@sha256:" + "4" * 64],
            },
            {
                "service": "ruciodb",
                "reference": "docker.io/postgres:14",
                "image_id": "sha256:" + "1" * 64,
                "repo_digests": ["docker.io/library/postgres@sha256:" + "2" * 64],
            },
        ],
    }
    manifest = reporter.parse_run_image_manifest(
        reporter.IMAGE_MANIFEST_PREFIX + json.dumps(payload),
        "runtime:test",
    )

    assert manifest.runtime_image_id == "sha256:" + "0" * 64
    assert manifest.runtime_probe == _runtime_probe()
    assert manifest.dependency_images == (
        reporter.DependencyImage(
            service="ruciodb",
            reference="docker.io/postgres:14",
            image_id="sha256:" + "1" * 64,
            repo_digests=("docker.io/library/postgres@sha256:" + "2" * 64,),
        ),
        reporter.DependencyImage(
            service="web1",
            reference="docker.io/rucio/test-webdav:latest",
            image_id="sha256:" + "3" * 64,
            repo_digests=("docker.io/rucio/test-webdav@sha256:" + "4" * 64,),
        ),
    )
    assert reporter.dependency_image_fingerprint(manifest.dependency_images) != reporter.dependency_image_fingerprint(
        (
            reporter.DependencyImage(
                service="ruciodb",
                reference="docker.io/postgres:14",
                image_id="sha256:" + "9" * 64,
                repo_digests=manifest.dependency_images[0].repo_digests,
            ),
            manifest.dependency_images[1],
        )
    )

    payload["runtime"]["image_id"] = "sha256:not-an-image-id"
    with pytest.raises(ValueError, match="incomplete image identity"):
        reporter.parse_run_image_manifest(
            reporter.IMAGE_MANIFEST_PREFIX + json.dumps(payload),
            "runtime:test",
        )


def test_control_environment_marker_tracks_pinned_requirements(tmp_path: Path) -> None:
    requirements = tmp_path / "requirements.txt"
    requirements.write_text("pytest==7.4.3\nPyYAML==6.0.3  # control parser\n")
    first = reporter.control_environment_marker(requirements)

    assert reporter.pinned_control_requirements(requirements) == {
        "PyYAML": "6.0.3",
        "pytest": "7.4.3",
    }
    assert first.startswith(reporter.CONTROL_VENV_MARKER_PREFIX)
    requirements.write_text("pytest==7.4.4\n")
    assert first != reporter.control_environment_marker(requirements)


def test_performance_outcome_plugin_marks_non_strict_xpass() -> None:
    class _Outcome:
        @staticmethod
        def get_result():
            return type("Report", (), {"when": "call", "passed": True, "wasxfail": "known issue"})()

    item = type("Item", (), {"user_properties": []})()
    hook = performance_outcomes.pytest_runtest_makereport(item, None)
    next(hook)
    with pytest.raises(StopIteration):
        hook.send(_Outcome())

    assert item.user_properties == [(reporter.JUNIT_OUTCOME_PROPERTY, "xpassed")]


def test_performance_outcome_property_reaches_xdist_junit(tmp_path: Path) -> None:
    test_file = tmp_path / "test_xpass.py"
    test_file.write_text(
        "import pytest\n\n"
        "@pytest.mark.xfail(reason='known issue')\n"
        "def test_unexpected_pass():\n"
        "    pass\n"
    )
    junit = tmp_path / "junit.xml"
    environment = dict(os.environ)
    environment.update(
        {
            "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
            "PYTHONPATH": str(Path(__file__).resolve().parents[2]),
        }
    )
    result = subprocess.run(  # noqa: S603
        [
            sys.executable,
            "-m",
            "pytest",
            "-p",
            "xdist.plugin",
            "-p",
            "tests.ruciopytest.performance_outcomes",
            "-n",
            "2",
            f"--rootdir={tmp_path}",
            f"--junitxml={junit}",
            str(test_file),
        ],
        cwd=tmp_path,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    testcase = ElementTree.parse(junit).find(".//testcase")  # noqa: S314
    assert testcase is not None
    properties = {
        (node.get("name"), node.get("value"))
        for node in testcase.findall("./properties/property")
    }
    assert (reporter.JUNIT_OUTCOME_PROPERTY, "xpassed") in properties


@pytest.mark.parametrize(
    ("project", "network"),
    (
        ("rucio-test-remote-dbs-py310-postgres14-0123abcd", "different-network"),
        ("unsafe-project", "unsafe-project-network"),
    ),
)
def test_project_cleanup_rejects_noncanonical_identity(
    project: str,
    network: str,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setattr(
        reporter,
        "docker_resource_names",
        lambda *args, **kwargs: pytest.fail("unsafe cleanup reached Docker inspection"),
    )

    cleanup = reporter.cleanup_compose_project(project, network)

    assert cleanup == reporter.DockerCleanup(complete=False, removed_residual_volume_count=0)
    assert "refusing to clean" in capsys.readouterr().err


def test_deferred_termination_signals_restore_handlers(monkeypatch) -> None:
    previous = {signum: object() for signum in reporter.termination_signals()}
    current = dict(previous)
    monkeypatch.setattr(reporter.signal, "getsignal", current.__getitem__)
    monkeypatch.setattr(reporter.signal, "signal", lambda signum, handler: current.__setitem__(signum, handler))

    with pytest.raises(KeyboardInterrupt, match=str(int(reporter.signal.SIGTERM))):
        with reporter.deferred_termination_signals():
            current[reporter.signal.SIGTERM](reporter.signal.SIGTERM, None)
            current[reporter.signal.SIGINT](reporter.signal.SIGINT, None)
            assert all(callable(current[signum]) for signum in reporter.termination_signals())

    assert current == previous


def test_run_tests_forces_runner_and_finishes_cleanup_after_repeated_signals(
    tmp_path: Path,
    monkeypatch,
) -> None:
    events = []

    class _InterruptedOutput:
        def __iter__(self):
            return self

        def __next__(self):
            raise KeyboardInterrupt("first signal")

    class _Process:
        pid = 123
        stdout = _InterruptedOutput()

    def stop_runner(_process) -> None:
        reporter.signal.getsignal(reporter.signal.SIGTERM)(reporter.signal.SIGTERM, None)
        events.append("graceful-stop-finished")

    def cleanup(_project: str, _network: str):
        reporter.signal.getsignal(reporter.signal.SIGINT)(reporter.signal.SIGINT, None)
        events.append("cleanup-finished")
        return reporter.DockerCleanup(complete=True, removed_residual_volume_count=0)

    monkeypatch.setattr(reporter.subprocess, "Popen", lambda *args, **kwargs: _Process())
    monkeypatch.setattr(reporter, "stop_runner", stop_runner)
    monkeypatch.setattr(reporter, "force_stop_runner", lambda process: events.append("forced-stop"))
    monkeypatch.setattr(reporter, "cleanup_compose_project", cleanup)

    workspace = tmp_path / "workspace"
    workspace.mkdir()
    xml_path = workspace / "junit.xml"
    with pytest.raises(KeyboardInterrupt):
        reporter.run_tests(
            workspace,
            xml_path,
            (),
            2,
            Path("/control/bin/python"),
            "runtime:test",
            "linux/arm64",
            (),
            _case_identity(),
            "4" * 64,
        )

    assert events == ["graceful-stop-finished", "forced-stop", "cleanup-finished"]


def test_runtime_build_identity_includes_native_platform(tmp_path: Path) -> None:
    context = tmp_path / "context"
    context.mkdir()
    (context / "runtime.Dockerfile").write_text("FROM scratch\n")
    arm_fingerprint = reporter.runtime_build_fingerprint(context, "linux/arm64")
    amd_fingerprint = reporter.runtime_build_fingerprint(context, "linux/amd64")
    build = reporter.RuntimeBuild(
        context=context,
        fingerprint=arm_fingerprint,
        image=f"runtime:py310-arm64-{arm_fingerprint[:16]}",
        platform="linux/arm64",
    )

    assert arm_fingerprint != amd_fingerprint
    assert "--platform=linux/arm64" in reporter.runtime_build_command("/usr/bin/docker", build)
    assert reporter.validate_runtime_platform(" Linux/ARM64\n") == "linux/arm64"
    with pytest.raises(SystemExit, match="invalid server platform"):
        reporter.validate_runtime_platform("arm64")


def test_resolved_case_and_harness_fingerprints_track_benchmark_inputs(
    tmp_path: Path,
    monkeypatch,
) -> None:
    assert reporter.resolved_case_identity(Path(__file__).resolve().parents[2], Path(sys.executable)) == _case_identity()

    harness = tmp_path / "harness"
    harness.mkdir()
    source = harness / "runner.py"
    source.write_text("first\n")
    monkeypatch.setattr(reporter, "BENCHMARK_HARNESS_INPUTS", (Path("harness"),))
    first = reporter.benchmark_harness_fingerprint(tmp_path)
    source.write_text("second\n")
    assert reporter.benchmark_harness_fingerprint(tmp_path) != first

    records = (reporter.TestTiming(classname="tests.test_ping", name="test_ping", duration=0.1),)
    assert reporter.test_manifest_fingerprint(records) != reporter.test_manifest_fingerprint(
        (replace(records[0], name="test_other"),)
    )


def test_source_fingerprint_is_stable_for_untracked_file_order() -> None:
    first = reporter.UntrackedFile(path="a.txt", mode=0o644, kind="file", digest="a" * 64)
    second = reporter.UntrackedFile(path="b.txt", mode=0o755, kind="symlink", digest="b" * 64)

    fingerprint = reporter.source_fingerprint("deadbeef", b"patch", (first, second))

    assert fingerprint == reporter.source_fingerprint("deadbeef", b"patch", (second, first))
    assert fingerprint != reporter.source_fingerprint("deadbeef", b"other", (first, second))


def test_load_junit_report_parses_statuses_and_wall_time(tmp_path: Path) -> None:
    junit = tmp_path / "junit.xml"
    junit.write_text(
        """<testsuites>
  <testsuite name="first" time="4.25">
    <testcase classname="tests.test_slow.TestThing" name="slow" time="1.23456">
      <failure type="AssertionError" message="failed">traceback</failure>
    </testcase>
    <testcase classname="tests.test_fast" name="fast" time="0.1">
      <skipped type="pytest.skip" message="not here" />
    </testcase>
    <testcase classname="tests.test_expected" name="expected_failure" time="0.2">
      <skipped type="pytest.xfail" message="known issue" />
    </testcase>
    <testcase classname="tests.test_unexpected" name="unexpected_pass" time="0.3">
      <properties><property name="rucio.test.outcome" value="xpassed" /></properties>
    </testcase>
    <testcase classname="tests.test_legacy" name="legacy_skip" time="0.05">
      <skipped message="no subtype" />
    </testcase>
  </testsuite>
  <testsuite name="second" time="2.0" />
</testsuites>
"""
    )

    report = reporter.load_junit_report(junit)

    assert report.wall_time == 4.25
    assert report.records == (
        reporter.TestTiming(
            classname="tests.test_legacy",
            name="legacy_skip",
            duration=0.05,
            status="skipped",
            message="no subtype",
        ),
        reporter.TestTiming(
            classname="tests.test_fast",
            name="fast",
            duration=0.1,
            status="skipped",
            status_type="pytest.skip",
            message="not here",
        ),
        reporter.TestTiming(
            classname="tests.test_expected",
            name="expected_failure",
            duration=0.2,
            status="xfailed",
            status_type="pytest.xfail",
            message="known issue",
        ),
        reporter.TestTiming(
            classname="tests.test_unexpected",
            name="unexpected_pass",
            duration=0.3,
            status="xpassed",
        ),
        reporter.TestTiming(
            classname="tests.test_slow.TestThing",
            name="slow",
            duration=1.235,
            status="failure",
            status_type="AssertionError",
            message="failed",
            detail="traceback",
        ),
    )


def test_metadata_records_canonical_case_and_residual_cleanup(tmp_path: Path) -> None:
    output = tmp_path / "metadata.json"
    snapshot = reporter.SourceSnapshot(
        branch="feature/report",
        revision="a" * 40,
        patch=b"",
        untracked_files=(),
        fingerprint="b" * 64,
    )
    reporter.write_metadata(
        reporter.JUnitReport(
            records=(
                reporter.TestTiming(classname="tests.test_ping", name="ping", duration=1.0),
                reporter.TestTiming(classname="tests.test_skip", name="skip", duration=0.0, status="skipped"),
                reporter.TestTiming(classname="tests.test_xfail", name="xfail", duration=0.0, status="xfailed"),
                reporter.TestTiming(classname="tests.test_xpass", name="xpass", duration=0.1, status="xpassed"),
            ),
            wall_time=1.5,
        ),
        _run_result(),
        _runtime_environment(),
        output,
        datetime(2026, 7, 16, 12, 0, tzinfo=timezone.utc),
        snapshot,
    )

    metadata = json.loads(output.read_text())
    assert metadata["schema_version"] == 4
    assert metadata["case"]["name"] == "remote-dbs-py310-postgres14"
    assert metadata["isolation"]["removed_residual_volume_count"] == 3
    assert "removed_volume_count" not in metadata["isolation"]
    assert metadata["dependencies"]["images"][0]["service"] == "ruciodb"
    assert metadata["comparability"]["environment_fingerprint"] in metadata["comparability"]["key"]
    assert metadata["comparability"]["environment_identity"]["schema"] == 2
    assert metadata["comparability"]["environment_identity"]["case"]["name"] == reporter.CASE_NAME
    assert metadata["comparability"]["environment_identity"]["dependency_fingerprint"] == metadata["dependencies"]["fingerprint"].removeprefix("sha256:")
    assert metadata["comparability"]["harness_fingerprint"] == "sha256:" + "4" * 64
    assert metadata["comparability"]["test_manifest_fingerprint"] == "sha256:" + "5" * 64
    assert metadata["execution"] == {"command": "python -m pytest <selection>", "exit_code": 0}
    assert metadata["results"] == {
        "tests": 4,
        "passed": 1,
        "skipped": 1,
        "xfailed": 1,
        "xpassed": 1,
        "failed": 0,
        "errors": 0,
    }
    assert metadata["comparability"]["comparable"] is True
    assert metadata["comparability"]["application_source_in_key"] is False
    assert output.stat().st_mode & 0o777 == 0o600


def test_runtime_identity_and_failed_execution_control_comparability() -> None:
    environment = _runtime_environment()
    result = _run_result()
    assert reporter.comparison_key(environment, result) != reporter.comparison_key(
        replace(environment, image_id="sha256:" + "9" * 64),
        result,
    )
    assert reporter.comparison_key(environment, result) != reporter.comparison_key(
        replace(environment, docker_server_cpu_count=16),
        result,
    )
    assert reporter.comparison_key(environment, result) != reporter.comparison_key(
        environment,
        replace(result, harness_fingerprint="6" * 64),
    )
    assert reporter.comparison_key(environment, result) != reporter.comparison_key(
        environment,
        replace(result, test_manifest_fingerprint="7" * 64),
    )
    assert reporter.comparison_key(environment, result) != reporter.comparison_key(
        environment,
        replace(result, case_identity=replace(result.case_identity, test_paths=("tests/test_ping.py",))),
    )
    failed = _run_result(returncode=3)
    assert reporter.comparison_invalid_reasons(environment, failed) == ["canonical runner exited with code 3"]
    diagnostic = _run_result(selectors=("tests/test_ping.py::test_rucio_ping_rest",))
    assert reporter.comparison_invalid_reasons(environment, diagnostic) == [
        "diagnostic selection is not a full autotest case"
    ]

    incomplete = replace(environment, docker_server_kernel="")
    assert "runtime or Docker server identity is incomplete" in reporter.comparison_invalid_reasons(
        incomplete,
        replace(result, runtime_image_id=incomplete.image_id, dependency_images=incomplete.dependency_images),
    )

    mismatched_manifest = replace(result, runtime_image_id="sha256:" + "9" * 64)
    assert reporter.comparison_invalid_reasons(environment, mismatched_manifest) == [
        "captured run image manifest does not match the inspected environment"
    ]
    assert reporter.comparison_invalid_reasons(environment, replace(result, harness_fingerprint="")) == [
        "benchmark case, harness, or test manifest identity is incomplete"
    ]


def test_render_html_is_complete_and_escapes_report_data() -> None:
    report = reporter.JUnitReport(
        records=(
            reporter.TestTiming(classname="tests.test_fast", name="fast", duration=0.5),
            reporter.TestTiming(
                classname="tests.test_slow.TestThing",
                name="</script><b>",
                duration=1.5,
                status="failure",
                message="bad",
            ),
            reporter.TestTiming(classname="tests.test_expected", name="xfail", duration=0.2, status="xfailed"),
            reporter.TestTiming(classname="tests.test_unexpected", name="xpass", duration=0.3, status="xpassed"),
        ),
        wall_time=2.5,
    )
    snapshot = reporter.SourceSnapshot(
        branch="feature/report",
        revision="a" * 40,
        patch=b"diff",
        untracked_files=(),
        fingerprint="b" * 64,
    )

    html = reporter.render_html(
        report,
        case="<case>",
        environment=_runtime_environment(),
        generated_at=datetime(2026, 7, 16, 12, 0, tzinfo=timezone.utc),
        source=Path("timings.json"),
        source_snapshot=snapshot,
        result=_run_result(selectors=("tests/test_ping.py",)),
    )

    assert "@@CASE@@" not in html
    assert "&lt;case&gt;" in html
    assert "&lt;/script&gt;&lt;b&gt;" in html
    assert "<\\/script><b>" in html
    assert "Diagnostic: tests/test_ping.py" in html
    assert "remote-dbs-py310-postgres14" in html
    assert "2 fixed" in html
    assert "Xfailed</span><strong>1" in html
    assert "Xpassed</span><strong>1" in html


def test_render_html_handles_test_durations_rounded_to_zero() -> None:
    report = reporter.JUnitReport(
        records=(reporter.TestTiming(classname="tests.test_fast", name="fast", duration=0.0),),
        wall_time=0.1,
    )
    snapshot = reporter.SourceSnapshot(
        branch="feature/report",
        revision="a" * 40,
        patch=b"",
        untracked_files=(),
        fingerprint="b" * 64,
    )

    html = reporter.render_html(
        report,
        case=reporter.CASE_NAME,
        environment=_runtime_environment(),
        generated_at=datetime(2026, 7, 16, 12, 0, tzinfo=timezone.utc),
        source=Path("junit.xml"),
        source_snapshot=snapshot,
        result=_run_result(),
    )

    assert "Tail share</span><strong>0.0%" in html
    assert "0.00%" in html
