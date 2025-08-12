#!/usr/bin/env python3
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

import argparse
import itertools
import json
import multiprocessing
import os
import pathlib
import re
import shutil
import subprocess  # noqa: S404 -- subprocess used for external commands
import sys
import time
import traceback
import uuid
from datetime import datetime
from tempfile import NamedTemporaryFile
from typing import Optional

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
DEFAULT_MATRIX_FILE = REPO_ROOT / 'etc/docker/test/matrix.json'
DEFAULT_NIGHTLY_MATRIX_FILE = REPO_ROOT / 'etc/docker/test/matrix_nightly.json'
DEFAULT_VO_MATRIX_FILE = REPO_ROOT / 'etc/docker/test/matrix_vo.json'
DEFAULT_COMPOSE_FILE = REPO_ROOT / 'etc/docker/dev/docker-compose.yml'
DEFAULT_TESTS_COMPOSE_FILE = REPO_ROOT / 'etc/docker/dev/docker-compose.tests.yml'
DEFAULT_RUCIO_TAG = 'latest'
DEFAULT_INTEGRATION_PROFILES = ('storage', 'externalmetadata', 'iam')
UNIT_PYTHON_VERSIONS = ('3.9', '3.10', '3.11', '3.12')
INTEGRATION_CORE_TESTS = (
    'tests/test_rucio_server.py',
    'tests/test_upload.py',
    'tests/test_impl_upload_download.py',
    'tests/test_rse_protocol_gfal2_impl.py',
    'tests/test_rse_protocol_xrootd.py',
    'tests/test_rse_protocol_ssh.py',
    'tests/test_rse_protocol_rsync.py',
    'tests/test_rse_protocol_rclone.py',
    'tests/test_conveyor.py',
)
INTEGRATION_POST_TESTS = (
    'tests/test_reaper.py::test_deletion_with_tokens',
    'tests/test_download.py::test_download_from_archive_on_xrd',
    'tests/test_did_meta_plugins.py::TestDidMetaMongo',
    'tests/test_did_meta_plugins.py::TestDidMetaExternalPostgresJSON',
    'tests/test_did_meta_plugins.py::TestDidMetaElastic',
)
SOURCE_VOLUME_TEMPLATE = (
    ('', '/rucio_source', ':ro'),
    ('tools', '/opt/rucio/tools', ':Z'),
    ('bin', '/opt/rucio/bin', ':Z'),
    ('lib', '/opt/rucio/lib', ':Z'),
    ('tests', '/opt/rucio/tests', ':Z'),
    ('etc/mail_templates', '/opt/rucio/etc/mail_templates', ':Z'),
    ('etc/automatix.json', '/opt/rucio/etc/automatix.json', ':Z'),
    ('etc/google-cloud-storage-test.json', '/opt/rucio/etc/google-cloud-storage-test.json', ':Z'),
    ('etc/idpsecrets.json', '/opt/rucio/etc/idpsecrets.json', ':Z'),
    ('etc/rse_repository.json', '/opt/rucio/etc/rse_repository.json', ':Z'),
    ('etc/docker/test/matrix_policy_package_tests.yml', '/opt/rucio/etc/docker/test/matrix_policy_package_tests.yml', ':Z'),
)
RUNTIME_IMAGE_ENV_BY_PYTHON = {
    '3.9': 'RUCIO_CI_RUNTIME_IMAGE_PY39',
    '3.10': 'RUCIO_CI_RUNTIME_IMAGE_PY310',
}
CI_MODE_DEFINITIONS = {
    'autotest': {
        'description': 'Workflow-aligned suite: autotest matrix (etc/docker/test/matrix.json)',
        'kind': 'matrix',
        'matrix_parse': DEFAULT_MATRIX_FILE,
    },
    'autotest-nightly': {
        'description': 'Workflow-aligned suite: nightly autotest matrix (etc/docker/test/matrix_nightly.json)',
        'kind': 'matrix',
        'matrix_parse': DEFAULT_NIGHTLY_MATRIX_FILE,
    },
    'vo': {
        'description': 'Workflow-aligned suite: VO matrix (etc/docker/test/matrix_vo.json)',
        'kind': 'matrix',
        'matrix_parse': DEFAULT_VO_MATRIX_FILE,
    },
    'integration': {
        'description': 'Workflow-aligned suite: integration workflow',
        'kind': 'integration',
    },
    'unit': {
        'description': 'Workflow-aligned suite: unit tests (tests/rucio)',
        'kind': 'unit',
    },
    'all': {
        'description': 'Workflow-aligned suite: all (autotest + vo + integration + unit)',
        'kind': 'sequence',
        'sequence': ('autotest', 'vo', 'integration', 'unit'),
    },
}


def ci_mode_choices() -> tuple[str, ...]:
    return tuple(CI_MODE_DEFINITIONS.keys())


def ci_mode_metadata() -> list[dict[str, str]]:
    return [{'mode': mode, 'description': str(config.get('description', ''))} for mode, config in CI_MODE_DEFINITIONS.items() if config.get('description')]


def env_enabled(name: str) -> bool:
    return os.environ.get(name) == '1'


def resolved_rucio_tag(cli_value: str) -> str:
    normalized = cli_value.strip()
    if normalized:
        return normalized
    return os.environ.get('RUCIO_TAG', DEFAULT_RUCIO_TAG)


def integration_project_name() -> str:
    project = os.environ.get('RUCIO_TEST_PROJECT_NAME', 'dev').strip()
    if not project:
        return 'dev'
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]*', project):
        raise RuntimeError(
            f"Invalid RUCIO_TEST_PROJECT_NAME '{project}'. Allowed: letters, digits, '-' and '_'."
        )
    return project


def source_volume_mounts() -> list[str]:
    source_root = pathlib.Path(os.path.abspath(os.curdir))
    return [
        f'{source_root if not relative_path else source_root / relative_path}:{container_path}{suffix}'
        for relative_path, container_path, suffix in SOURCE_VOLUME_TEMPLATE
    ]


def docker_volume_args() -> list[str]:
    return list(itertools.chain.from_iterable(('-v', mount) for mount in source_volume_mounts()))


def default_runtime_image() -> str:
    return (
        f"docker.io/{os.environ.get('DOCKER_REPO', 'rucio')}/rucio-dev:"
        f"{os.environ.get('RUCIO_DEV_PREFIX', '')}{os.environ.get('RUCIO_TAG', DEFAULT_RUCIO_TAG)}"
    )


def load_json_file(path: pathlib.Path):
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except OSError as error:
        raise RuntimeError(f"Could not read JSON file '{path}'.") from error
    except json.JSONDecodeError as error:
        raise RuntimeError(f"Invalid JSON in '{path}'.") from error


def load_matrix_cases(path: pathlib.Path) -> list[dict]:
    loaded = load_json_file(path)
    if not isinstance(loaded, list):
        raise RuntimeError(f"Matrix file '{path}' must contain a JSON list of objects.")
    if not all(isinstance(entry, dict) for entry in loaded):
        raise RuntimeError(f"Matrix file '{path}' must contain only JSON objects.")
    return loaded


def plan_unit_python_versions() -> tuple[str, ...]:
    return UNIT_PYTHON_VERSIONS


def run(*args, check=True, return_stdout=False, env=None, cwd: Optional[pathlib.Path] = None) -> Optional[bytes]:
    kwargs = {'check': check, 'stdout': sys.stderr, 'stderr': subprocess.STDOUT}
    if env is not None:
        kwargs['env'] = env
    if cwd is not None:
        kwargs['cwd'] = str(cwd)
    if return_stdout:
        kwargs['stderr'] = sys.stderr
        kwargs['stdout'] = subprocess.PIPE
    args = [str(a) for a in args]
    print("** Running", " ".join(map(lambda a: repr(a) if ' ' in a else a, args)), kwargs, file=sys.stderr, flush=True)
    proc = subprocess.run(args, **kwargs)
    if return_stdout:
        return proc.stdout


def env_args(caseenv):
    return [*itertools.chain.from_iterable((('--env', f'{k}={v}') for k, v in caseenv.items())), '--env', 'GITHUB_ACTIONS']


def matches(small: dict, group: dict):
    return all(key in group and small[key] == group[key] for key in small)


def stringify_dict(inp: dict):
    return {str(k): str(v) for k, v in inp.items()}


def find_image(images: dict, case: dict):
    for image, idgroup in images.items():
        if matches(idgroup, case):
            return image
    raise RuntimeError("Could not find image for case " + str(case))


def case_id(case: dict) -> str:
    parts = [case["DIST"], 'py' + case["PYTHON"], case["SUITE"], case.get("RDBMS", "")]
    return '-'.join(filter(bool, parts))


def case_log(caseid, msg, file=sys.stderr):
    print(caseid, msg, file=file, flush=True)


def run_tests(cases: list, images: dict, tests: Optional[list[str]] = None):
    use_podman = env_enabled('USE_PODMAN')
    parallel = env_enabled('PARALLEL_AUTOTESTS')
    failfast = env_enabled('PARALLEL_AUTOTESTS_FAILFAST')
    copy_rucio_logs = env_enabled('COPY_AUTOTEST_LOGS')
    logs_dir = pathlib.Path('.autotest')
    if parallel or copy_rucio_logs:
        logs_dir.mkdir(exist_ok=True)

    def gen_case_kwargs(case: dict):
        use_httpd = case.get('RUN_HTTPD', True)
        return {
            'caseenv': stringify_dict(case),
            'image': find_image(images=images, case=case),
            'use_podman': use_podman,
            'use_namespace': use_podman and parallel,
            'use_httpd': use_httpd,
            'copy_rucio_logs': copy_rucio_logs and use_httpd,
            'logs_dir': logs_dir / f'log-{case_id(case)}',
            'tests': tests or [],
        }

    if parallel:
        parallel_num = min(int(os.environ.get('PARALLEL_AUTOTESTS_PROCNUM', 3)), len(cases))
        with multiprocessing.Pool(processes=parallel_num, maxtasksperchild=1) as prpool:
            tasks = [
                (
                    _case,
                    prpool.apply_async(
                        run_case_logger,
                        (),
                        {'run_case_kwargs': gen_case_kwargs(_case), 'stdlog': logs_dir / f'log-{case_id(_case)}.txt'},
                    ),
                )
                for _case in cases
            ]
            start_time = time.time()
            for _case, task in tasks:
                timeleft = start_time + 21600 - time.time()  # 6 hour overall timeout
                if timeleft <= 0:
                    print(
                        "Timeout exceeded, still running:",
                        list(map(lambda t: case_id(t[0]), filter(lambda t: not t[1].ready(), tasks))),
                        file=sys.stderr,
                        flush=True,
                    )
                    prpool.close()
                    sys.exit(1)

                # throwing an exception in the task will not exit task.get immediately, so a success variable is used
                success = task.get(timeout=timeleft)
                if not success and failfast:
                    prpool.close()
                    sys.exit(1)
    else:
        for _case in cases:
            run_case(**gen_case_kwargs(_case))


def run_case_logger(run_case_kwargs: dict, stdlog=sys.stderr):
    caseid = case_id(run_case_kwargs['caseenv'])
    case_log(caseid, 'started task. Logging to ' + repr(stdlog))
    defaultstderr = sys.stderr
    startmsg = f'{("=" * 80)}\nStarting test case {caseid}\n  at {datetime.now().isoformat()}\n{"=" * 80}\n'

    def run_logged_case(log_target) -> bool:
        sys.stderr = log_target
        try:
            run_case(**run_case_kwargs)
            return True
        except Exception:
            traceback.print_exc(file=sys.stderr)
            case_log(caseid, f'errored with {sys.exc_info()[0].__name__}: {sys.exc_info()[1]}', file=defaultstderr)
            return False
        finally:
            sys.stderr = defaultstderr

    if isinstance(stdlog, pathlib.PurePath):
        with open(str(stdlog), 'a') as logfile:
            logfile.write(startmsg)
            logfile.flush()
            success = run_logged_case(logfile)
    else:
        print(startmsg, file=stdlog)
        success = run_logged_case(stdlog)

    if success:
        case_log(caseid, 'completed successfully!')
    return success


def run_case(caseenv, image, use_podman, use_namespace, use_httpd, copy_rucio_logs, logs_dir: pathlib.Path, tests: list[str]):
    if use_namespace:
        namespace = str(uuid.uuid4())
        namespace_args = ['--namespace', namespace]
        namespace_env = {"NAMESPACE": namespace}
    else:
        namespace_args = []
        namespace_env = {}

    pod = ""
    if use_podman:
        print("*** Starting with pod for", {**caseenv, "IMAGE": image}, file=sys.stderr, flush=True)
        stdout = run('podman', *namespace_args, 'pod', 'create', return_stdout=True)
        pod = stdout.decode().strip()
        if not pod:
            raise RuntimeError("Could not determine pod id")
    else:
        print("*** Starting", {**caseenv, "IMAGE": image}, file=sys.stderr, flush=True)

    try:
        if use_httpd:
            print("* Using httpd for test", file=sys.stderr, flush=True)
            success = run_with_httpd(
                caseenv=caseenv,
                image=image,
                namespace_env=namespace_env,
                copy_rucio_logs=copy_rucio_logs,
                logs_dir=logs_dir,
                tests=tests,
            )
        else:
            print("* Running test directly without httpd", file=sys.stderr, flush=True)
            success = run_test_directly(
                caseenv=caseenv,
                image=image,
                use_podman=use_podman,
                pod=pod,
                namespace_args=namespace_args,
                tests=tests,
            )
    finally:
        print("*** Finalizing", {**caseenv, "IMAGE": image}, file=sys.stderr, flush=True)
        if pod:
            run('podman', *namespace_args, 'pod', 'stop', '-t', '10', pod, check=False)
            run('podman', *namespace_args, 'pod', 'rm', '--force', pod, check=False)

    if not success:
        sys.exit(1)


def run_test_directly(
        caseenv: dict[str, str],
        image: str,
        use_podman: bool,
        pod: str,
        namespace_args: list[str],
        tests: list[str],
):
    pod_net_arg = ['--pod', pod] if use_podman else []
    scripts_to_run = ' && '.join([
        'rm -rf /tmp/rucio_source',
        'mkdir -p /tmp/rucio_source',
        'cp -a /rucio_source/. /tmp/rucio_source/',
        'cd /tmp/rucio_source',
        'ln -sf pyproject.server.toml pyproject.toml',
        'pip install --no-cache-dir -e /tmp/rucio_source',
        'RUCIO_SOURCE_DIR=/tmp/rucio_source python3 tools/test/suite_runner.py --run-suite-from-env',
    ])

    try:
        if tests:
            caseenv = {**caseenv, 'TESTS': ' '.join(tests)}

        run(
            'docker',
            *namespace_args,
            'run',
            '--rm',
            *pod_net_arg,
            *docker_volume_args(),
            *(env_args(caseenv)),
            image,
            'sh',
            '-c',
            scripts_to_run,
        )

        return True
    except subprocess.CalledProcessError as error:
        print(
            f"** Running tests '{error.cmd}' exited with code {error.returncode}",
            {**caseenv, "IMAGE": image},
            file=sys.stderr,
            flush=True,
        )
    return False


def run_with_httpd(
        caseenv: dict[str, str],
        image: str,
        namespace_env: dict[str, str],
        copy_rucio_logs: bool,
        logs_dir: pathlib.Path,
        tests: list[str],
) -> bool:
    if not DEFAULT_TESTS_COMPOSE_FILE.exists():
        raise RuntimeError(f"Docker Compose tests override file not found at '{DEFAULT_TESTS_COMPOSE_FILE}'.")

    rdbms = caseenv.get('RDBMS', '')
    project = os.urandom(8).hex()
    compose_env = os.environ.copy()
    compose_env.update(namespace_env)
    compose_env.update(caseenv)
    compose_env['RUCIO_IMAGE'] = image
    compose_env['RUCIO_SOURCE_MOUNT_MODE'] = ':ro'
    up_down_args = [
        '--file', str(DEFAULT_COMPOSE_FILE),
        '--file', str(DEFAULT_TESTS_COMPOSE_FILE),
    ]
    if rdbms:
        up_down_args.extend(['--profile', rdbms])
    compose_cmd = ['docker', 'compose', '-p', project, *up_down_args]
    try:
        run(*compose_cmd, 'up', '-d', env=compose_env)
        run(
            *compose_cmd,
            'exec',
            '-T',
            'rucio',
            'bash',
            '-lc',
            (
                'set -euo pipefail; '
                'rm -rf /tmp/rucio_source; '
                'mkdir -p /tmp/rucio_source; '
                'cp -a /rucio_source/. /tmp/rucio_source/; '
                'cd /tmp/rucio_source; '
                'ln -sf pyproject.server.toml pyproject.toml; '
                'pip install --no-cache-dir -e /tmp/rucio_source'
            ),
        )
        suite_env = list(itertools.chain.from_iterable(('-e', f'{k}={v}') for k, v in caseenv.items()))
        suite_env.extend(['-e', 'RUCIO_SOURCE_DIR=/tmp/rucio_source'])
        if tests:
            suite_env.extend(['-e', f"TESTS={' '.join(tests)}"])
        run(
            *compose_cmd,
            'exec',
            '-T',
            *suite_env,
            'rucio',
            'bash',
            '-lc',
            'cd /tmp/rucio_source && python3 tools/test/suite_runner.py --run-suite-from-env',
        )
        return True
    except subprocess.CalledProcessError as error:
        print(
            f"** Process '{error.cmd}' exited with code {error.returncode}",
            {**caseenv, "IMAGE": image},
            file=sys.stderr,
            flush=True,
        )
    finally:
        run(*compose_cmd, 'logs', 'rucio', check=False)
        if copy_rucio_logs:
            try:
                if logs_dir.exists():
                    shutil.rmtree(logs_dir)
                container_id_raw = run(*compose_cmd, 'ps', '-q', 'rucio', return_stdout=True, check=False)
                container_id = (container_id_raw or b'').decode().strip()
                if container_id:
                    run('docker', 'cp', f'{container_id}:/var/log', str(logs_dir))
            except Exception:
                print(
                    "** Error on retrieving logs for",
                    {**caseenv, "IMAGE": image},
                    '\n',
                    traceback.format_exc(),
                    '\n**',
                    file=sys.stderr,
                    flush=True,
                )
        run(*compose_cmd, 'down', '-t', '30', check=False, env=compose_env)
    return False


def current_git_sha() -> str:
    git = shutil.which('git')
    if not git:
        return 'unknown'
    proc = subprocess.run(
        [git, 'rev-parse', 'HEAD'],
        cwd=REPO_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        return 'unknown'
    return proc.stdout.strip() or 'unknown'


def resolve_image_digest(image: str) -> str:
    docker = shutil.which('docker')
    if not docker:
        return 'unavailable (docker not found)'
    proc = subprocess.run(
        [docker, 'image', 'inspect', image, '--format', '{{index .RepoDigests 0}}'],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        return 'unavailable (image not present locally)'
    digest = proc.stdout.strip()
    return digest or 'unavailable (no repo digest metadata)'


def ensure_docker_ready() -> None:
    if not shutil.which('docker'):
        raise RuntimeError("Required command 'docker' is not available.")
    run('docker', 'info')


def parse_cli_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run workflow-aligned CI suites/modes, print workflow matrices, and execute explicit JSON payloads."
        )
    )
    parser.add_argument('--print-matrix', action='store_true', help='Print parsed matrix JSON from --matrix-file.')
    parser.add_argument('--print-nightly-matrix', action='store_true', help='Print parsed nightly matrix JSON.')
    parser.add_argument('--matrix-file', type=pathlib.Path, default=DEFAULT_MATRIX_FILE, help='Input matrix file (JSON list of objects).')
    parser.add_argument('--print-unit-python-versions', action='store_true', help='Print unit test python versions JSON array.')
    parser.add_argument('--print-unit-test-matrix', action='store_true', help='Print unit workflow matrix JSON object.')
    parser.add_argument('--run-matrix-case', default='', help='Run one matrix case from a JSON object.')
    parser.add_argument('--run-payload-json', default='', help='Run explicit payload JSON object.')
    parser.add_argument('--list-modes', action='store_true', help='Print CI mode metadata JSON.')
    parser.add_argument('--print-resolved-ci-images', action='store_true', help='Print resolved CI runtime images JSON.')
    parser.add_argument(
        '--mode',
        choices=ci_mode_choices(),
        help='CI suite mode for local workflow-aligned runs.',
    )
    parser.add_argument('--filter', default='', help='Optional pytest filter expression.')
    parser.add_argument('--runtime-image-py39', default='', help='Runtime image for Python 3.9 matrix entries.')
    parser.add_argument('--runtime-image-py310', default='', help='Runtime image for Python 3.10 matrix entries.')
    parser.add_argument('--integration-runtime-image', default='', help='Runtime image for integration suites.')
    parser.add_argument('--rucio-tag', default='', help=f"Tag used for sidecar images in docker-compose (default: {DEFAULT_RUCIO_TAG}).")
    return parser.parse_args()


def detect_compose_command() -> list[str]:
    docker = shutil.which('docker')
    if docker and subprocess.run([docker, 'compose', 'version'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False).returncode == 0:
        return [docker, 'compose']
    docker_compose = shutil.which('docker-compose')
    if docker_compose:
        return [docker_compose]
    raise RuntimeError("Neither 'docker-compose' nor 'docker compose' is available.")


def run_payload(obj: dict) -> None:
    cases = (obj["matrix"],) if isinstance(obj["matrix"], dict) else obj["matrix"]

    if "runtime_images" in obj:
        runtime_images = obj["runtime_images"]
        images = {
            runtime_images[python_version]: {"PYTHON": python_version}
            for case in cases
            for python_version in [case.get("PYTHON", "3.9")]
            if python_version in runtime_images
        }
    else:
        images = obj["images"]

    tests = obj.get("tests") or []
    if isinstance(tests, str):
        tests = [tests]

    run_tests(cases, images, tests=tests)


def parse_json_object(raw_value: str, option_name: str) -> dict:
    try:
        loaded = json.loads(raw_value)
    except json.JSONDecodeError as error:
        raise RuntimeError(f"Invalid JSON for {option_name}.") from error
    if not isinstance(loaded, dict):
        raise RuntimeError(f"{option_name} expects a JSON object.")
    return loaded


def run_matrix_case(case: dict, filter_expr: str, runtime_image_py39: str, runtime_image_py310: str) -> None:
    py39_image, py310_image, _ = resolved_ci_images(runtime_image_py39, runtime_image_py310, '')
    payload = {
        'matrix': case,
        'runtime_images': runtime_images_for_cases([case], py39_image, py310_image),
    }
    if filter_expr:
        payload['tests'] = [filter_expr]
    run_payload(payload)


def runtime_image_for_python(
    python_version: str,
    runtime_image_py39: str,
    runtime_image_py310: str,
    default_image: str,
) -> str:
    selected_env_name = RUNTIME_IMAGE_ENV_BY_PYTHON.get(python_version)
    if selected_env_name == 'RUCIO_CI_RUNTIME_IMAGE_PY310':
        return runtime_image_py310
    if selected_env_name == 'RUCIO_CI_RUNTIME_IMAGE_PY39':
        return runtime_image_py39
    return default_image


def runtime_images_for_cases(cases: list[dict], runtime_image_py39: str, runtime_image_py310: str) -> dict[str, str]:
    default_image = default_runtime_image()
    python_versions = {str(case.get('PYTHON', '3.9')) for case in cases}
    return {
        python_version: runtime_image_for_python(
            python_version=python_version,
            runtime_image_py39=runtime_image_py39,
            runtime_image_py310=runtime_image_py310,
            default_image=default_image,
        )
        for python_version in sorted(python_versions)
    }


def build_payload(matrix: list[dict], runtime_image_py39: str, runtime_image_py310: str, tests: list[str]) -> dict:
    payload = {
        'matrix': matrix,
        'runtime_images': runtime_images_for_cases(matrix, runtime_image_py39, runtime_image_py310),
    }
    if tests:
        payload['tests'] = tests
    return payload


def run_unit_tests_in_image(image: str, filter_expr: str) -> None:
    pytest_cmd: list[str] = ['python3', '-m', 'pytest', 'tests/rucio']
    if filter_expr:
        pytest_cmd.extend(['-k', filter_expr])
    run(
        'docker',
        'run',
        '--rm',
        '-v',
        f'{REPO_ROOT}:/rucio_source',
        '-w',
        '/rucio_source',
        image,
        *pytest_cmd,
    )


def integration_init_script() -> str:
    return """
set -e
rm -rf /tmp/rucio_source
mkdir -p /tmp/rucio_source
cp -a /rucio_source/. /tmp/rucio_source/
cd /tmp/rucio_source
cp etc/rse-accounts.cfg.template /opt/rucio/etc/rse-accounts.cfg
cp etc/rse-accounts.cfg.template /opt/rucio/etc/rse-accounts.cfg.template
cp etc/rse_repository.json /opt/rucio/etc/rse_repository.json
cp etc/rclone-init.cfg /opt/rucio/etc/rclone-init.cfg
ln -sf pyproject.server.toml pyproject.toml
ln -sf /root/.ssh/ruciouser_sshkey /root/.ssh/id_rsa 2>/dev/null || true
ln -sf /root/.ssh/ruciouser_sshkey.pub /root/.ssh/id_rsa.pub 2>/dev/null || true
pip install --no-cache-dir -e /tmp/rucio_source
RUCIO_SOURCE_DIR=/tmp/rucio_source tools/run_tests.sh -ir
""".strip()


def remove_project_db_volume(project: str) -> None:
    volume_name = f'{project}_vol-ruciodb-data'
    attached_containers_raw = run(
        'docker',
        'ps',
        '-aq',
        '--filter',
        f'volume={volume_name}',
        check=False,
        return_stdout=True,
    ) or b''
    attached_containers = [container_id for container_id in attached_containers_raw.decode().splitlines() if container_id]
    if attached_containers:
        run('docker', 'rm', '-f', *attached_containers, check=False)
    run('docker', 'volume', 'rm', '-f', volume_name, check=False)


def run_integration_pytest(compose_exec_args: list[str], *pytest_args: str, context: str) -> None:
    cmd = [
        *compose_exec_args,
        '-w',
        '/tmp/rucio_source',
        '-e',
        'RUCIO_SOURCE_DIR=/tmp/rucio_source',
        'rucio',
        'tools/pytest.sh',
        '-v',
        '--tb=short',
        *pytest_args,
    ]
    try:
        run(*cmd)
    except subprocess.CalledProcessError as error:
        print(f"::error::Integration pytest step failed: {context}", file=sys.stderr, flush=True)
        print(f"::group::Integration debug rerun: {context}", file=sys.stderr, flush=True)
        run(
            *compose_exec_args,
            '-w',
            '/tmp/rucio_source',
            '-e',
            'RUCIO_SOURCE_DIR=/tmp/rucio_source',
            'rucio',
            'tools/pytest.sh',
            '-vv',
            '--tb=long',
            '-x',
            '-rA',
            *pytest_args,
            check=False,
        )
        print("::endgroup::", file=sys.stderr, flush=True)
        raise error


def run_integration_suite(runtime_image: str, filter_expr: str) -> None:
    compose_cmd = detect_compose_command()
    if not DEFAULT_COMPOSE_FILE.exists():
        raise RuntimeError(f"Docker Compose file not found at '{DEFAULT_COMPOSE_FILE}'.")

    project = integration_project_name()
    compose_env = os.environ.copy()
    compose_env['RUCIO_SOURCE_MOUNT_MODE'] = ':ro'

    compose_profiles_args = list(itertools.chain.from_iterable(('--profile', profile) for profile in DEFAULT_INTEGRATION_PROFILES))
    pull_compose_files_args = ['--file', str(DEFAULT_COMPOSE_FILE)]
    compose_files_args = ['--file', str(DEFAULT_COMPOSE_FILE)]
    compose_project_args = ['-p', project]
    tmp_override = None

    if runtime_image:
        with NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as override:
            override.write(f"services:\n  rucio:\n    image: {runtime_image}\n")
            tmp_override = override.name
        compose_files_args.extend(['--file', tmp_override])

    should_skip_pull = env_enabled('RUCIO_AUTOTEST_REUSE_IMAGES')

    try:
        run(*compose_cmd, *compose_project_args, *compose_files_args, *compose_profiles_args, 'down', check=False, env=compose_env)
        print(f"** Resetting '{project}_vol-ruciodb-data' before integration startup", file=sys.stderr, flush=True)
        remove_project_db_volume(project)
        if not should_skip_pull:
            run(*compose_cmd, *compose_project_args, *pull_compose_files_args, *compose_profiles_args, 'pull', env=compose_env)

        compose_env['DEV_PROFILES'] = ','.join(DEFAULT_INTEGRATION_PROFILES)
        run(*compose_cmd, *compose_project_args, *compose_files_args, *compose_profiles_args, 'up', '-d', env=compose_env)
        compose_exec_args = [*compose_cmd, *compose_project_args, *compose_files_args, *compose_profiles_args, 'exec', '-T']

        run(*compose_exec_args, 'rucio', 'bash', '-c', integration_init_script())

        if filter_expr:
            run_integration_pytest(compose_exec_args, '-k', filter_expr, context=f"-k {filter_expr}")
            return

        for target in INTEGRATION_CORE_TESTS:
            run_integration_pytest(compose_exec_args, target, context=target)

        run_integration_pytest(compose_exec_args, '--export-artifacts-from=test_tpc', 'tests/test_tpc.py', context='tests/test_tpc.py')
        fts_log_file = run(*compose_exec_args, 'rucio', 'cat', '/tmp/test_tpc.artifact', return_stdout=True).decode().replace('\r', '').strip()
        if not fts_log_file:
            raise RuntimeError(f"Could not read /tmp/test_tpc.artifact from 'rucio' service in project '{project}'")

        run(
            *compose_exec_args,
            'fts',
            '/bin/bash',
            '-c',
            f"shopt -s nullglob; files=({fts_log_file}); (( ${{#files[@]}} )) && grep -Fq '3rd pull' \"${{files[@]}}\"",
        )

        for target in INTEGRATION_POST_TESTS:
            run_integration_pytest(compose_exec_args, target, context=target)
    finally:
        run(*compose_cmd, *compose_project_args, *compose_files_args, *compose_profiles_args, 'logs', 'rucio', check=False)
        run(*compose_cmd, *compose_project_args, *compose_files_args, *compose_profiles_args, 'down', check=False, env=compose_env)
        if tmp_override:
            try:
                pathlib.Path(tmp_override).unlink(missing_ok=True)
            except TypeError:
                if pathlib.Path(tmp_override).exists():
                    pathlib.Path(tmp_override).unlink()


def resolved_ci_images(runtime_image_py39: str, runtime_image_py310: str, integration_runtime_image: str) -> tuple[str, str, str]:
    default_image = default_runtime_image()
    py39_image = runtime_image_py39 or os.environ.get('RUCIO_CI_RUNTIME_IMAGE_PY39', default_image)
    py310_image = runtime_image_py310 or os.environ.get('RUCIO_CI_RUNTIME_IMAGE_PY310', default_image)
    integration_image = integration_runtime_image or os.environ.get('RUCIO_INTEGRATION_RUNTIME_IMAGE', default_image)
    return py39_image, py310_image, integration_image


def run_mode(
    mode: str,
    filter_expr: str,
    runtime_image_py39: str,
    runtime_image_py310: str,
    integration_runtime_image: str,
    rucio_tag: str,
) -> None:
    py39_image, py310_image, integration_image = resolved_ci_images(runtime_image_py39, runtime_image_py310, integration_runtime_image)
    tests = [filter_expr] if filter_expr else []
    git_sha = current_git_sha()

    print(
        (
            f"** CI mode context: mode={mode} git_sha={git_sha} filter={filter_expr or '<none>'} "
            f"test_project={integration_project_name()} rucio_tag={rucio_tag}"
        ),
        file=sys.stderr,
        flush=True,
    )
    print(
        f"** CI runtime images: py39={py39_image} ({resolve_image_digest(py39_image)}), "
        f"py310={py310_image} ({resolve_image_digest(py310_image)}), "
        f"integration={integration_image} ({resolve_image_digest(integration_image)})",
        file=sys.stderr,
        flush=True,
    )

    stack = [mode]
    while stack:
        selected_mode = stack.pop()
        mode_cfg = CI_MODE_DEFINITIONS.get(selected_mode)
        if not mode_cfg:
            raise RuntimeError(f"Unsupported mode '{selected_mode}'.")

        mode_kind = mode_cfg.get('kind')
        if mode_kind == 'matrix':
            matrix_file = mode_cfg.get('matrix_parse')
            if not isinstance(matrix_file, pathlib.Path):
                raise RuntimeError(f"Mode '{selected_mode}' has no matrix parser configuration.")
            matrix = load_matrix_cases(matrix_file)
            run_payload(build_payload(matrix, py39_image, py310_image, tests))
            continue

        if mode_kind == 'integration':
            run_integration_suite(integration_image, filter_expr)
            continue

        if mode_kind == 'unit':
            run_unit_tests_in_image(py39_image, filter_expr)
            continue

        if mode_kind == 'sequence':
            sequence = mode_cfg.get('sequence')
            if not sequence:
                raise RuntimeError(f"Mode '{selected_mode}' has no sequence configuration.")
            stack.extend(reversed([str(nested_mode) for nested_mode in sequence]))
            continue

        raise RuntimeError(f"Unsupported kind '{mode_kind}' for mode '{selected_mode}'.")


def main():
    args = parse_cli_args()
    os.chdir(REPO_ROOT)
    os.environ['RUCIO_TAG'] = resolved_rucio_tag(args.rucio_tag)

    if args.print_matrix:
        print(json.dumps(load_matrix_cases(args.matrix_file)))
        return

    if args.print_nightly_matrix:
        print(json.dumps(load_matrix_cases(DEFAULT_NIGHTLY_MATRIX_FILE)))
        return

    if args.print_unit_python_versions:
        print(json.dumps(list(plan_unit_python_versions())))
        return

    if args.print_unit_test_matrix:
        print(json.dumps({'python-version': list(plan_unit_python_versions())}))
        return

    if args.list_modes:
        print(json.dumps(ci_mode_metadata()))
        return

    if args.print_resolved_ci_images:
        py39_image, py310_image, integration_image = resolved_ci_images(
            runtime_image_py39=args.runtime_image_py39,
            runtime_image_py310=args.runtime_image_py310,
            integration_runtime_image=args.integration_runtime_image,
        )
        print(json.dumps({
            'default': default_runtime_image(),
            'py39': py39_image,
            'py310': py310_image,
            'integration': integration_image,
            'rucio_tag': os.environ['RUCIO_TAG'],
        }))
        return

    if args.mode:
        ensure_docker_ready()
        run_mode(
            mode=args.mode,
            filter_expr=args.filter,
            runtime_image_py39=args.runtime_image_py39,
            runtime_image_py310=args.runtime_image_py310,
            integration_runtime_image=args.integration_runtime_image,
            rucio_tag=os.environ['RUCIO_TAG'],
        )
        return

    if args.run_matrix_case:
        case = parse_json_object(args.run_matrix_case, '--run-matrix-case')
        ensure_docker_ready()
        run_matrix_case(
            case=case,
            filter_expr=args.filter,
            runtime_image_py39=args.runtime_image_py39,
            runtime_image_py310=args.runtime_image_py310,
        )
        return

    if args.run_payload_json:
        payload = parse_json_object(args.run_payload_json, '--run-payload-json')
        ensure_docker_ready()
        run_payload(payload)
        return

    raise RuntimeError(
        "No execution command specified. Use --mode, --run-matrix-case, or --run-payload-json."
    )


if __name__ == "__main__":
    main()
