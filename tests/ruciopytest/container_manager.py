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

import fcntl
import hashlib
import json
import os
import re
import secrets
import stat
import subprocess  # noqa: S404
import sys
import threading
from typing import TYPE_CHECKING

from tools.devenv.build_context import temporary_docker_context
from tools.devenv.runtime import RUNTIME_INPUTS

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from pathlib import Path
    from types import TracebackType
    from typing import Optional, TextIO

    from .profiles import TestCase


REPORT_IMAGE_MANIFEST_ENV = "RUCIO_TEST_REPORT_IMAGE_MANIFEST"
REPORT_IMAGE_MANIFEST_PREFIX = "RUCIO_TEST_IMAGE_MANIFEST="
REPORT_SOURCE_UID_ENV = "RUCIO_TEST_REPORT_SOURCE_UID"
REPORT_SOURCE_GID_ENV = "RUCIO_TEST_REPORT_SOURCE_GID"


def _validate_test_log_component(value: str) -> None:
    separators = tuple(separator for separator in (os.sep, os.altsep) if separator)
    if not value or value in {".", ".."} or "\0" in value or any(separator in value for separator in separators):
        raise ValueError("Test log project and file names must be single path components")


def _open_test_log_directory(parent_fd: int, name: str) -> int:
    try:
        os.mkdir(name, mode=0o700, dir_fd=parent_fd)
    except FileExistsError:
        pass

    descriptor = os.open(
        name,
        os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0),
        dir_fd=parent_fd,
    )
    try:
        opened = os.fstat(descriptor)
        linked = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        if (
            not stat.S_ISDIR(opened.st_mode)
            or opened.st_uid != os.getuid()
            or (opened.st_dev, opened.st_ino) != (linked.st_dev, linked.st_ino)
        ):
            raise RuntimeError("Refusing an unsafe Rucio test log directory")
        os.fchmod(descriptor, 0o700)
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def open_test_log_file(
    root_dir: "Path",
    project_name: str,
    filename: str,
    mode: str,
) -> "TextIO":
    """Open a private test log without following or reopening filesystem links."""
    _validate_test_log_component(project_name)
    _validate_test_log_component(filename)
    if mode not in {"a", "w"}:
        raise ValueError("Test logs support only append and write modes")
    if (
        os.open not in os.supports_dir_fd
        or os.mkdir not in os.supports_dir_fd
        or os.stat not in os.supports_dir_fd
        or os.stat not in os.supports_follow_symlinks
        or not hasattr(os, "O_DIRECTORY")
        or not hasattr(os, "O_NOFOLLOW")
        or not hasattr(os, "O_NONBLOCK")
        or not hasattr(os, "getuid")
    ):
        raise RuntimeError("Secure Rucio test logs require POSIX directory descriptors")

    directory_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
    root_fd = log_root_fd = log_dir_fd = file_fd = None
    try:
        root_fd = os.open(root_dir.resolve(), directory_flags)
        log_root_fd = _open_test_log_directory(root_fd, ".test-logs")
        log_dir_fd = _open_test_log_directory(log_root_fd, project_name)
        file_flags = os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
        file_flags |= os.O_NONBLOCK
        if mode == "a":
            file_flags |= os.O_APPEND
        file_fd = os.open(filename, file_flags, 0o600, dir_fd=log_dir_fd)
        opened = os.fstat(file_fd)
        linked = os.stat(filename, dir_fd=log_dir_fd, follow_symlinks=False)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_nlink != 1
            or opened.st_uid != os.getuid()
            or (opened.st_dev, opened.st_ino) != (linked.st_dev, linked.st_ino)
        ):
            raise RuntimeError("Refusing an unsafe Rucio test log file")
        os.fchmod(file_fd, 0o600)
        if mode == "w":
            os.ftruncate(file_fd, 0)
        output = os.fdopen(file_fd, mode, encoding="utf-8")
        file_fd = None
        return output
    except OSError as error:
        raise RuntimeError("Could not safely open a Rucio test log") from error
    finally:
        for descriptor in (file_fd, log_dir_fd, log_root_fd, root_fd):
            if descriptor is not None:
                os.close(descriptor)


REPORT_RUNTIME_PROBE = r"""
import importlib.metadata
import json
import os
import platform
from pathlib import Path

def read_value(path):
    try:
        return Path(path).read_text().strip()
    except OSError:
        return ""

cpu_quota = None
cpu_limit_source = ""
cpu_max = read_value("/sys/fs/cgroup/cpu.max")
if cpu_max:
    cpu_limit_source = "cgroup-v2"
    quota, period = cpu_max.split()
    if quota != "max":
        cpu_quota = float(quota) / float(period)
else:
    quota = read_value("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
    period = read_value("/sys/fs/cgroup/cpu/cpu.cfs_period_us")
    if quota and period:
        cpu_limit_source = "cgroup-v1"
        if int(quota) > 0:
            cpu_quota = int(quota) / int(period)

memory_limit = read_value("/sys/fs/cgroup/memory.max")
memory_limit_source = "cgroup-v2" if memory_limit else ""
if not memory_limit:
    memory_limit = read_value("/sys/fs/cgroup/memory/memory.limit_in_bytes")
    memory_limit_source = "cgroup-v1" if memory_limit else ""
memory_limit_bytes = None if memory_limit in {"", "max"} else int(memory_limit)
if memory_limit_bytes is not None and memory_limit_bytes >= 1 << 60:
    memory_limit_bytes = None

affinity = len(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else None
print(json.dumps({
    "system": platform.system(),
    "release": platform.release(),
    "machine": platform.machine(),
    "python": platform.python_version(),
    "cpu_count": os.cpu_count(),
    "affinity_count": affinity,
    "cpu_quota": cpu_quota,
    "cpu_limit_source": cpu_limit_source,
    "memory_limit_bytes": memory_limit_bytes,
    "memory_limit_source": memory_limit_source,
    "xdist_version": importlib.metadata.version("pytest-xdist"),
}))
"""


def checkout_id(root_dir: "Path") -> str:
    return hashlib.sha256(str(root_dir.resolve()).encode()).hexdigest()[:8]


def database_schema_id(root_dir: "Path") -> str:
    root_dir = root_dir.resolve()
    sqla_dir = root_dir / "lib/rucio/db/sqla"
    schema_paths = [
        sqla_dir / "constants.py",
        sqla_dir / "models.py",
        sqla_dir / "types.py",
        *(sqla_dir / "migrate_repo/versions").glob("*.py"),
    ]
    digest = hashlib.sha256(str(root_dir).encode())
    for path in sorted(schema_paths):
        if path.is_file():
            digest.update(str(path.relative_to(root_dir)).encode())
            digest.update(path.read_bytes())
    return digest.hexdigest()[:8]


class ContainerManager:
    COMPOSE_FILES = (
        "etc/docker/dev/docker-compose.yml",
        "etc/docker/dev/docker-compose.test.yml",
    )
    RUNTIME_DOCKERFILE = "etc/docker/test/runtime.Dockerfile"
    _locally_built_images: set[tuple[str, str, str, str]] = set()
    _image_build_lock = threading.Lock()
    _editable_install_lock = threading.Lock()

    def __init__(
        self,
        case: "TestCase",
        root_dir: "Path",
        *,
        keep_db: bool = False,
        image: "Optional[str]" = None,
        environ: "Optional[Mapping[str, str]]" = None,
        log_output: bool = False,
    ) -> None:
        self.case = case
        self.root_dir = root_dir.resolve()
        self.keep_db = keep_db
        self._base_environment = dict(os.environ if environ is None else environ)
        supplied_image = (
            image
            or self._base_environment.get(case.runtime_image_variable)
            or self._base_environment.get("RUCIO_TEST_IMAGE")
        )
        self.image = supplied_image or f"rucio-test-runtime:{checkout_id(self.root_dir)}-{case.runtime}"
        self.build_local = supplied_image is None
        nonce = self._base_environment.get("RUCIO_TEST_PROJECT_NONCE")
        if nonce is not None and re.fullmatch(r"[0-9a-f]{8}", nonce) is None:
            raise ValueError("RUCIO_TEST_PROJECT_NONCE must contain exactly eight lowercase hexadecimal characters")
        self.project_name = self.make_project_name(
            case.id,
            self.root_dir,
            reusable=keep_db,
            nonce=nonce,
        )
        self.log_dir = self.root_dir / ".test-logs" / self.project_name
        self.output_log = self.log_dir / "case.log" if log_output else None
        self._stopped = False
        self._lock_handle = None
        self._source_owner = self._validated_source_owner()

        self.environment = dict(self._base_environment)
        self.environment.pop("COMPOSE_PROFILES", None)
        self.environment.pop("DOCKER_DEFAULT_PLATFORM", None)
        self.environment.update(case.env_vars)
        self.environment.update(
            {
                "RUCIO_TEST_IMAGE": self.image,
                "RUCIO_TEST_IMAGE_TARGET": case.runtime_target,
                "RUCIO_NETWORK_NAME": f"{self.project_name}-network",
                "DEV_PROFILES": ",".join(case.compose_profiles),
                "RUCIO_HOME": case.env_vars.get("RUCIO_HOME", "/opt/rucio"),
            }
        )

    @staticmethod
    def make_project_name(
        case_id: str,
        root_dir: "Path",
        *,
        reusable: bool,
        nonce: "Optional[str]" = None,
    ) -> str:
        slug = "".join(character if character.isalnum() else "-" for character in case_id.lower()).strip("-")
        if reusable:
            suffix = database_schema_id(root_dir)
        else:
            suffix = nonce or secrets.token_hex(4)
        return f"rucio-test-{slug[:40]}-{suffix}"

    def __enter__(self) -> "ContainerManager":
        self.start()
        return self

    def __exit__(
        self,
        exc_type: "Optional[type[BaseException]]",
        exc_value: "Optional[BaseException]",
        traceback: "Optional[TracebackType]",
    ) -> None:
        self.stop(check=exc_type is None)

    def start(self) -> None:
        self._acquire_project_lock()
        try:
            if self.output_log:
                with open_test_log_file(self.root_dir, self.project_name, self.output_log.name, "w"):
                    pass
                print(f"Case output: {self.output_log}", flush=True)
            self._set_native_platform()
            if self.build_local:
                image_key = (
                    str(self.root_dir),
                    self.image,
                    self.case.runtime_target,
                    self.case.runtime_platform or self.environment["RUCIO_TEST_NATIVE_PLATFORM"],
                )
                with self._image_build_lock:
                    if image_key not in self._locally_built_images:
                        self._build_image()
                        self._locally_built_images.add(image_key)
            self._pull_dependencies()
            self._run(self.compose_command("up", "-d", "--wait", "--wait-timeout", "180"), timeout=240)
            self._emit_image_manifest()
            with self._editable_install_lock:
                self.exec(
                    "rucio",
                    "python",
                    "-m",
                    "pip",
                    "install",
                    "--no-deps",
                    "--no-build-isolation",
                    "-e",
                    "/rucio_source",
                    timeout=180,
                )
            self.exec("rucio", "httpd", "-k", "graceful", timeout=30)
            self.exec(
                "rucio",
                "curl",
                "--fail",
                "--silent",
                "--show-error",
                "--retry",
                "15",
                "--retry-all-errors",
                "--retry-delay",
                "2",
                "--insecure",
                "https://localhost/ping",
                timeout=60,
            )
        except BaseException:
            self.stop(check=False)
            raise

    def stop(self, *, check: bool = True) -> None:
        if self._stopped:
            return
        cleanup_failed = False
        cleanup_error = None
        try:
            ownership_restored = self._restore_source_ownership()
            self.capture_logs()
            command = self.compose_command(
                "down",
                "--timeout",
                "30",
                "--remove-orphans",
            )
            if not self.keep_db:
                command.append("--volumes")
            result = self._run(command, check=False, timeout=120)
            volumes_removed = result.returncode == 0 and (not self.keep_db or self._remove_non_database_volumes())
            self._stopped = result.returncode == 0 and volumes_removed and ownership_restored
            cleanup_failed = not self._stopped
        except (OSError, RuntimeError, subprocess.SubprocessError) as error:
            cleanup_failed = True
            cleanup_error = error
        finally:
            self._release_project_lock()
        if cleanup_failed:
            message = f"Failed to clean up test project {self.project_name}"
            if check:
                raise RuntimeError(message) from cleanup_error
            print(f"Warning: {message}", file=sys.stderr)

    def _validated_source_owner(self) -> "Optional[tuple[str, str]]":
        uid = self._base_environment.get(REPORT_SOURCE_UID_ENV)
        gid = self._base_environment.get(REPORT_SOURCE_GID_ENV)
        if uid is None and gid is None:
            return None
        if (
            uid is None
            or gid is None
            or not uid.isdecimal()
            or not gid.isdecimal()
            or not hasattr(os, "getuid")
            or not hasattr(os, "getgid")
            or int(uid) != os.getuid()
            or int(gid) != os.getgid()
        ):
            raise ValueError("Report source ownership must match the invoking host user")
        return uid, gid

    def _restore_source_ownership(self) -> bool:
        if self._source_owner is None:
            return True
        uid, gid = self._source_owner
        result = self.exec(
            "rucio",
            "chown",
            "--recursive",
            "--no-dereference",
            f"{uid}:{gid}",
            "--",
            "/rucio_source",
            check=False,
            timeout=120,
        )
        return result.returncode == 0

    def _service_image_record(self, service: str, reference: str) -> dict[str, object]:
        containers = self._run(
            self.compose_command("ps", "--all", "--quiet", service),
            capture_output=True,
            timeout=30,
        )
        container_ids = [value for value in containers.stdout.splitlines() if value]
        if len(container_ids) != 1:
            raise RuntimeError(f"Expected exactly one running container for Compose service {service}")
        container = self._run(
            ("docker", "container", "inspect", "--format", "{{json .}}", container_ids[0]),
            capture_output=True,
            timeout=30,
        )
        try:
            image_id = json.loads(container.stdout).get("Image")
        except (AttributeError, json.JSONDecodeError) as error:
            raise RuntimeError(f"Could not inspect the running image for Compose service {service}") from error
        if not isinstance(image_id, str) or re.fullmatch(r"sha256:[0-9a-f]{64}", image_id) is None:
            raise RuntimeError(f"Docker did not report the running image for Compose service {service}")
        image_result = self._run(
            ("docker", "image", "inspect", "--format", "{{json .}}", image_id),
            capture_output=True,
            timeout=30,
        )
        try:
            image = json.loads(image_result.stdout)
        except json.JSONDecodeError as error:
            raise RuntimeError(f"Could not inspect image {image_id} for Compose service {service}") from error
        if not isinstance(image, dict) or image.get("Id") != image_id:
            raise RuntimeError(f"Docker returned an inconsistent image identity for Compose service {service}")
        repo_digests = image.get("RepoDigests")
        return {
            "service": service,
            "reference": reference,
            "image_id": image_id,
            "repo_digests": sorted(value for value in repo_digests if isinstance(value, str)) if isinstance(repo_digests, list) else [],
        }

    def _runtime_environment_record(self) -> dict[str, object]:
        result = self.exec(
            "rucio",
            "/opt/venv/bin/python",
            "-c",
            REPORT_RUNTIME_PROBE,
            capture_output=True,
            timeout=30,
        )
        try:
            record = json.loads(result.stdout)
        except json.JSONDecodeError as error:
            raise RuntimeError("Could not inspect the running test container environment") from error
        if not isinstance(record, dict):
            raise RuntimeError("Running test container returned an invalid environment record")
        return record

    def _emit_image_manifest(self) -> None:
        if self._base_environment.get(REPORT_IMAGE_MANIFEST_ENV) != "1":
            return
        config_result = self._run(
            self.compose_command("config", "--format", "json"),
            capture_output=True,
            timeout=30,
        )
        try:
            services = json.loads(config_result.stdout).get("services")
        except (AttributeError, json.JSONDecodeError) as error:
            raise RuntimeError("Could not resolve the canonical Compose service map") from error
        if not isinstance(services, dict):
            raise RuntimeError("Canonical Compose config did not contain a service map")

        runtime = None
        dependencies = []
        for service, definition in sorted(services.items()):
            if not isinstance(service, str) or not isinstance(definition, dict):
                raise RuntimeError("Canonical Compose config contained an invalid service definition")
            if service != "rucio" and "build" in definition:
                continue
            reference = definition.get("image")
            if not isinstance(reference, str) or not reference:
                raise RuntimeError(f"Canonical Compose service {service} did not resolve to an image reference")
            record = self._service_image_record(service, reference)
            if service == "rucio":
                runtime = record
            else:
                dependencies.append(record)
        if runtime is None or not dependencies:
            raise RuntimeError("Canonical Compose run did not expose its runtime and dependency images")
        payload = json.dumps(
            {
                "runtime": runtime,
                "runtime_environment": self._runtime_environment_record(),
                "dependencies": dependencies,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        print(f"{REPORT_IMAGE_MANIFEST_PREFIX}{payload}", flush=True)

    def compose_command(self, *arguments: str) -> list[str]:
        command = ["docker", "compose", "-p", self.project_name]
        compose_files = list(self.COMPOSE_FILES)
        if self._base_environment.get("RUCIO_DEV_PROFILE") == "cpu":
            compose_files.append("etc/docker/dev/docker-compose.test-instrumentation.yml")
        if self._base_environment.get("RUCIO_DEV_DEBUG") == "1":
            compose_files.append("etc/docker/dev/docker-compose.test-debug.yml")
        if self._base_environment.get("RUCIO_DEV_OBSERVE") == "1":
            compose_files.append("etc/docker/dev/docker-compose.test-observe.yml")
        for compose_file in compose_files:
            command.extend(("-f", str(self.root_dir / compose_file)))
        for profile in self.case.compose_profiles:
            command.extend(("--profile", profile))
        command.extend(arguments)
        return command

    def exec(
        self,
        service: str,
        *arguments: str,
        environment: "Optional[Mapping[str, str]]" = None,
        check: bool = True,
        capture_output: bool = False,
        interactive: bool = False,
        timeout: "Optional[int]" = None,
    ) -> subprocess.CompletedProcess:
        command = self.compose_command("exec")
        if not interactive:
            command.append("-T")
        for key, value in (environment or {}).items():
            command.extend(("--env", f"{key}={value}"))
        command.extend((service, *arguments))
        return self._run(
            command,
            check=check,
            capture_output=capture_output,
            timeout=timeout,
        )

    def capture_logs(self) -> None:
        try:
            compose = self._run(
                self.compose_command("logs", "--no-color", "--timestamps"),
                check=False,
                capture_output=True,
                timeout=60,
            )
            httpd = self.exec(
                "rucio",
                "cat",
                "/var/log/rucio/httpd_error_log",
                check=False,
                capture_output=True,
                timeout=30,
            )
            if compose.stdout:
                with open_test_log_file(self.root_dir, self.project_name, "compose.log", "w") as output:
                    output.write(compose.stdout)
            if httpd.stdout:
                with open_test_log_file(self.root_dir, self.project_name, "httpd_error.log", "w") as output:
                    output.write(httpd.stdout)
        except (OSError, RuntimeError, subprocess.SubprocessError):
            pass

    def _acquire_project_lock(self) -> None:
        if not self.keep_db or self._lock_handle is not None:
            return
        lock_handle = open_test_log_file(self.root_dir, self.project_name, "project.lock", "w")
        try:
            fcntl.flock(lock_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            lock_handle.close()
            raise RuntimeError(f"Reusable test project {self.project_name} is already running") from error
        self._lock_handle = lock_handle

    def _release_project_lock(self) -> None:
        if self._lock_handle is None:
            return
        fcntl.flock(self._lock_handle, fcntl.LOCK_UN)
        self._lock_handle.close()
        self._lock_handle = None

    def _remove_non_database_volumes(self) -> bool:
        result = self._run(
            self.compose_command("config", "--format", "json"),
            check=False,
            capture_output=True,
            timeout=30,
        )
        if result.returncode:
            return False
        try:
            volumes = json.loads(result.stdout).get("volumes", {}).values()
        except (AttributeError, json.JSONDecodeError):
            return False
        removable = [volume["name"] for volume in volumes if volume.get("labels", {}).get("rucio.test.database") != self.case.rdbms]
        if not removable:
            return True
        result = self._run(
            ("docker", "volume", "rm", *removable),
            check=False,
            timeout=60,
        )
        return result.returncode == 0

    def _set_native_platform(self) -> None:
        if "RUCIO_TEST_NATIVE_PLATFORM" not in self.environment:
            result = self._run(
                (
                    "docker",
                    "version",
                    "--format",
                    "{{.Server.Os}}/{{.Server.Arch}}",
                ),
                capture_output=True,
                timeout=30,
            )
            platform = result.stdout.strip()
            if not platform:
                raise RuntimeError("Docker did not report its server platform")
            self.environment["RUCIO_TEST_NATIVE_PLATFORM"] = platform
        self.environment["RUCIO_TEST_IMAGE_PLATFORM"] = (
            self.case.runtime_platform or self.environment["RUCIO_TEST_NATIVE_PLATFORM"]
        )

    def _build_image(self) -> None:
        target = self.case.runtime_target
        platform = self.case.runtime_platform or self.environment["RUCIO_TEST_NATIVE_PLATFORM"]
        with temporary_docker_context(self.root_dir, RUNTIME_INPUTS) as context:
            command = [
                "docker",
                "buildx",
                "build",
                "--platform",
                platform,
                "--load",
                "--file",
                str(context / self.RUNTIME_DOCKERFILE),
                "--target",
                target,
                "--build-arg",
                f"PYTHON={self.case.python}",
                "--tag",
                self.image,
                str(context),
            ]
            self._run(command, timeout=3600)

    def _pull_dependencies(self) -> None:
        services = self._run(
            self.compose_command("config", "--services"),
            capture_output=True,
            timeout=30,
        ).stdout.split()
        dependencies = [service for service in services if service not in {"rucio", "workspace"}]
        if dependencies:
            self._run(
                self.compose_command("pull", "--policy", "always", *dependencies),
                timeout=900,
            )

    def _run(
        self,
        command: "Sequence[str]",
        *,
        check: bool = True,
        capture_output: bool = False,
        timeout: "Optional[int]" = None,
    ) -> subprocess.CompletedProcess:
        if self.output_log:
            if capture_output:
                result = subprocess.run(  # noqa: S603
                    command,
                    check=False,
                    capture_output=True,
                    cwd=self.root_dir,
                    env=self.environment,
                    text=True,
                    timeout=timeout,
                )
                with open_test_log_file(self.root_dir, self.project_name, self.output_log.name, "a") as output:
                    output.write(result.stdout or "")
                    output.write(result.stderr or "")
            else:
                with open_test_log_file(self.root_dir, self.project_name, self.output_log.name, "a") as output:
                    result = subprocess.run(  # noqa: S603
                        command,
                        check=False,
                        cwd=self.root_dir,
                        env=self.environment,
                        stdout=output,
                        stderr=subprocess.STDOUT,
                        text=True,
                        timeout=timeout,
                    )
            if check:
                result.check_returncode()
            return result
        return subprocess.run(  # noqa: S603
            command,
            check=check,
            capture_output=capture_output,
            cwd=self.root_dir,
            env=self.environment,
            text=True,
            timeout=timeout,
        )
