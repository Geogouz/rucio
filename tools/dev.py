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
import os
import pathlib
import re
import shlex
import shutil
import subprocess  # noqa: S404 - wrapper around external tools
import sys
from typing import Optional

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
BASE_COMPOSE_FILE = REPO_ROOT / 'etc/docker/dev/docker-compose.yml'
PORTS_COMPOSE_FILE = REPO_ROOT / 'etc/docker/dev/docker-compose.ports.yml'
TESTS_COMPOSE_FILE = REPO_ROOT / 'etc/docker/dev/docker-compose.tests.yml'
PROJECT_NAME_PATTERN = re.compile(r'[A-Za-z0-9][A-Za-z0-9_-]*')
DEFAULT_RUCIO_TAG = 'latest'


def run(*args: str, env: Optional[dict[str, str]] = None, check: bool = True, capture_stdout: bool = False) -> str:
    cmd = [str(arg) for arg in args]
    print(f"** Running {shlex.join(cmd)}", file=sys.stderr, flush=True)
    kwargs: dict = {
        'cwd': REPO_ROOT,
        'env': env,
        'text': True,
        'check': check,
    }
    if capture_stdout:
        kwargs['stdout'] = subprocess.PIPE
        kwargs['stderr'] = subprocess.PIPE
    proc = subprocess.run(cmd, **kwargs)
    return proc.stdout or ''


def python_bin() -> str:
    venv_python = REPO_ROOT / '.venv/bin/python'
    if venv_python.exists() and os.access(venv_python, os.X_OK):
        return str(venv_python)
    return 'python3'


def validate_project_name(project: str) -> str:
    project = project.strip()
    if not project:
        raise RuntimeError('Project name cannot be empty.')
    if not PROJECT_NAME_PATTERN.fullmatch(project):
        raise RuntimeError(f"Invalid project name '{project}'. Allowed: letters, digits, '-' and '_'.")
    return project


def resolve_rucio_tag(cli_tag: str) -> str:
    normalized = cli_tag.strip()
    if normalized:
        return normalized
    return os.environ.get('RUCIO_TAG', DEFAULT_RUCIO_TAG)


def detect_compose_command() -> list[str]:
    docker = shutil.which('docker')
    if docker and subprocess.run([docker, 'compose', 'version'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False).returncode == 0:
        return [docker, 'compose']
    docker_compose = shutil.which('docker-compose')
    if docker_compose:
        return [docker_compose]
    raise RuntimeError("Neither 'docker compose' nor 'docker-compose' is available.")


def compose_supports_pull_never(compose_cmd: list[str]) -> bool:
    return len(compose_cmd) == 2 and pathlib.Path(compose_cmd[0]).name == 'docker' and compose_cmd[1] == 'compose'


def normalize_profiles(profile_args: list[str]) -> list[str]:
    profiles: list[str] = []
    for profile_arg in profile_args:
        for profile in profile_arg.split(','):
            normalized = profile.strip()
            if normalized and normalized not in profiles:
                profiles.append(normalized)
    return profiles


def compose_prefix(
        compose_cmd: list[str],
        project: str,
        files: list[pathlib.Path],
        profiles: list[str],
) -> list[str]:
    args = [*compose_cmd, '--project-name', project]
    for compose_file in files:
        args.extend(['--file', str(compose_file)])
    for profile in profiles:
        args.extend(['--profile', profile])
    return args


def print_next_steps(project: str) -> None:
    print(
        f"\nLocal environment '{project}' is up.\n"
        "Next steps:\n"
        f"  - Shell in rucio: docker compose --project-name {project} --file {BASE_COMPOSE_FILE} exec rucio /bin/bash\n"
        f"  - Run local suite: ./tools/dev.py test local --project {project}-test\n"
        f"  - Show collected tests: ./tools/dev.py test local --project {project}-test --list-tests\n"
        f"  - Narrow run with filter: ./tools/dev.py test local --project {project}-test --filter 'tests/test_replica.py::TestReplicaCore::test_delete_replicas'\n"
        f"  - Tear down: ./tools/dev.py down --project {project}"
    )


def remove_project_db_volume(project: str) -> None:
    volume_name = f'{project}_vol-ruciodb-data'
    attached_raw = run(
        'docker',
        'ps',
        '-aq',
        '--filter',
        f'volume={volume_name}',
        check=False,
        capture_stdout=True,
    )
    attached = [container_id for container_id in attached_raw.splitlines() if container_id]
    if attached:
        run('docker', 'rm', '-f', *attached, check=False)
    run('docker', 'volume', 'rm', '-f', volume_name, check=False)


def remove_autotest_volumes() -> None:
    listed = run('docker', 'volume', 'ls', '--format', '{{.Name}}', check=False, capture_stdout=True)
    volumes = [name for name in listed.splitlines() if re.fullmatch(r'[0-9a-f]{16}_vol-.*', name)]
    if volumes:
        run('docker', 'volume', 'rm', *volumes, check=False)


def command_up(args: argparse.Namespace) -> None:
    project = validate_project_name(args.project)
    profiles = normalize_profiles(args.profile)
    files = [BASE_COMPOSE_FILE]
    if args.ports:
        files.append(PORTS_COMPOSE_FILE)

    compose_cmd = detect_compose_command()
    env = os.environ.copy()
    env['DEV_PROFILES'] = ','.join(profiles)
    env['RUCIO_TAG'] = resolve_rucio_tag(args.rucio_tag)
    prefix = compose_prefix(compose_cmd, project, files, profiles)

    if not args.skip_pull:
        run(*prefix, 'pull', env=env)

    up_args = [*prefix, 'up', '-d']
    if args.skip_pull and compose_supports_pull_never(compose_cmd):
        up_args.extend(['--pull', 'never'])
    run(*up_args, env=env)
    print_next_steps(project)


def command_down(args: argparse.Namespace) -> None:
    project = validate_project_name(args.project)
    files = [BASE_COMPOSE_FILE]
    if args.ports:
        files.append(PORTS_COMPOSE_FILE)
    compose_cmd = detect_compose_command()
    env = os.environ.copy()
    prefix = compose_prefix(compose_cmd, project, files, [])
    run(*prefix, 'down', check=False, env=env)


def command_clean(args: argparse.Namespace) -> None:
    project = validate_project_name(args.project)
    remove_project_db_volume(project)
    if args.autotest_volumes:
        remove_autotest_volumes()


def _prepare_writable_source(prefix: list[str], env: dict[str, str]) -> None:
    run(
        *prefix,
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
        env=env,
    )


def command_test_local(args: argparse.Namespace) -> None:
    project = validate_project_name(args.project)
    rdbms = args.rdbms.strip()
    if not rdbms:
        raise RuntimeError('RDBMS profile cannot be empty for local tests.')

    compose_cmd = detect_compose_command()
    files = [BASE_COMPOSE_FILE, TESTS_COMPOSE_FILE]
    profiles = [rdbms]
    prefix = compose_prefix(compose_cmd, project, files, profiles)
    env = os.environ.copy()
    env['DEV_PROFILES'] = rdbms
    env['RDBMS'] = rdbms
    env['RUCIO_SOURCE_MOUNT_MODE'] = ':ro'
    env['RUCIO_TAG'] = resolve_rucio_tag(args.rucio_tag)
    if args.runtime_image:
        env['RUCIO_IMAGE'] = args.runtime_image

    should_skip_pull = args.reuse_images
    try:
        run(*prefix, 'down', check=False, env=env)
        remove_project_db_volume(project)
        if not should_skip_pull:
            run(*prefix, 'pull', env=env)
        run(*prefix, 'up', '-d', env=env)

        _prepare_writable_source(prefix, env)

        exec_env_args = [
            '-e', 'SUITE=remote_dbs',
            '-e', f'RDBMS={rdbms}',
            '-e', 'RUCIO_SOURCE_DIR=/tmp/rucio_source',
        ]
        if args.filter:
            exec_env_args.extend(['-e', f'TESTS={args.filter}'])

        if args.list_tests:
            run(
                *prefix,
                'exec',
                '-T',
                *exec_env_args,
                'rucio',
                'bash',
                '-lc',
                'cd /tmp/rucio_source && tools/run_tests.sh -i',
                env=env,
            )
            collect_command = 'cd /tmp/rucio_source && tools/pytest.sh --collect-only -q'
            if args.filter:
                collect_command += f' -k {shlex.quote(args.filter)}'
            run(
                *prefix,
                'exec',
                '-T',
                *exec_env_args,
                'rucio',
                'bash',
                '-lc',
                collect_command,
                env=env,
            )
            return

        test_command = 'cd /tmp/rucio_source && tools/run_tests.sh'
        if args.filter:
            test_command += ' -p'

        run(
            *prefix,
            'exec',
            '-T',
            *exec_env_args,
            'rucio',
            'bash',
            '-lc',
            test_command,
            env=env,
        )
    finally:
        run(*prefix, 'logs', 'rucio', check=False, env=env)
        if not args.keep_up:
            run(*prefix, 'down', check=False, env=env)


def _run_ci_mode(
    mode: str,
    project: str,
    filter_expr: str,
    runtime_image: str,
    reuse_images: bool,
    rucio_tag: str,
) -> None:
    env = os.environ.copy()
    env['RUCIO_TEST_PROJECT_NAME'] = validate_project_name(project)
    resolved_tag = resolve_rucio_tag(rucio_tag)
    env['RUCIO_TAG'] = resolved_tag
    if reuse_images:
        env['RUCIO_AUTOTEST_REUSE_IMAGES'] = '1'

    command = [
        python_bin(),
        'tools/test/run_tests.py',
        '--mode',
        mode,
    ]
    if filter_expr:
        command.extend(['--filter', filter_expr])
    if runtime_image:
        command.extend(['--integration-runtime-image', runtime_image])
    command.extend(['--rucio-tag', resolved_tag])
    run(*command, env=env)


def command_test_ci(args: argparse.Namespace) -> None:
    _run_ci_mode(
        mode=args.mode,
        project=args.project,
        filter_expr=args.filter,
        runtime_image='',
        reuse_images=args.reuse_images,
        rucio_tag=args.rucio_tag,
    )


def command_test_integration(args: argparse.Namespace) -> None:
    _run_ci_mode(
        mode='integration',
        project=args.project,
        filter_expr=args.filter,
        runtime_image=args.runtime_image,
        reuse_images=args.reuse_images,
        rucio_tag=args.rucio_tag,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Minimal local interface for docker compose and workflow-aligned CI test execution.')
    subparsers = parser.add_subparsers(dest='command', required=True)

    up_parser = subparsers.add_parser('up', help='Start a compose stack.')
    up_parser.add_argument('--project', default='dev', help='Compose project name.')
    up_parser.add_argument('--profile', action='append', default=[], help='Compose profile; repeat or pass comma-separated values.')
    up_parser.add_argument('--ports', action='store_true', help='Include docker-compose.ports.yml.')
    up_parser.add_argument('--skip-pull', action='store_true', help='Skip docker compose pull.')
    up_parser.add_argument('--rucio-tag', default='', help=f"Tag used by compose images (default: {DEFAULT_RUCIO_TAG}).")
    up_parser.set_defaults(handler=command_up)

    down_parser = subparsers.add_parser('down', help='Stop and remove a compose stack.')
    down_parser.add_argument('--project', default='dev', help='Compose project name.')
    down_parser.add_argument('--ports', action='store_true', help='Include docker-compose.ports.yml.')
    down_parser.set_defaults(handler=command_down)

    clean_parser = subparsers.add_parser('clean', help='Remove local test volumes.')
    clean_parser.add_argument('--project', default='dev-test', help='Compose project name for DB volume cleanup.')
    clean_parser.add_argument('--autotest-volumes', action='store_true', help='Also remove generated autotest volumes.')
    clean_parser.set_defaults(handler=command_clean)

    test_parser = subparsers.add_parser('test', help='Run tests.')
    test_subparsers = test_parser.add_subparsers(dest='test_command', required=True)

    test_local = test_subparsers.add_parser('local', help='Run local remote_dbs suite in an isolated compose project.')
    test_local.add_argument('--project', default='dev-test', help='Compose project name.')
    test_local.add_argument('--rdbms', default='postgres14', help='RDBMS profile used for local test stack.')
    test_local.add_argument('--filter', default='', help='Pytest selector forwarded via TESTS.')
    test_local.add_argument('--runtime-image', default='', help='Runtime image for the rucio service.')
    test_local.add_argument('--rucio-tag', default='', help=f"Tag used by compose sidecar images (default: {DEFAULT_RUCIO_TAG}).")
    test_local.add_argument('--reuse-images', action='store_true', help='Skip pulling images before up.')
    test_local.add_argument('--keep-up', action='store_true', help='Do not tear down stack after test run.')
    test_local.add_argument('--list-tests', action='store_true', help='Bootstrap and print collected pytest tests instead of executing the suite.')
    test_local.set_defaults(handler=command_test_local)

    test_ci = test_subparsers.add_parser('ci', help='Run workflow-aligned CI mode through tools/test/run_tests.py.')
    test_ci.add_argument('--project', default='dev-test', help='Compose project name used by the workflow-aligned runner.')
    test_ci.add_argument(
        '--mode',
        default='autotest',
        choices=('autotest', 'autotest-nightly', 'vo', 'integration', 'unit', 'all'),
        help='Workflow mode (aligned with CI jobs, not guaranteed bit-for-bit parity).',
    )
    test_ci.add_argument('--filter', default='', help='Optional pytest selector.')
    test_ci.add_argument('--rucio-tag', default='', help=f"Tag used by compose sidecar images (default: {DEFAULT_RUCIO_TAG}).")
    test_ci.add_argument('--reuse-images', action='store_true', help='Set RUCIO_AUTOTEST_REUSE_IMAGES=1.')
    test_ci.set_defaults(handler=command_test_ci)

    test_integration = test_subparsers.add_parser('integration', help='Run workflow-aligned integration mode with optional runtime image override.')
    test_integration.add_argument('--project', default='dev-test', help='Compose project name used by integration runner.')
    test_integration.add_argument('--filter', default='', help='Optional pytest selector.')
    test_integration.add_argument('--runtime-image', default='', help='Integration runtime image override.')
    test_integration.add_argument('--rucio-tag', default='', help=f"Tag used by compose sidecar images (default: {DEFAULT_RUCIO_TAG}).")
    test_integration.add_argument('--reuse-images', action='store_true', help='Set RUCIO_AUTOTEST_REUSE_IMAGES=1.')
    test_integration.set_defaults(handler=command_test_integration)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.handler(args)


if __name__ == '__main__':
    main()
