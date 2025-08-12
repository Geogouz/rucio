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
import configparser
import functools
import glob
import json
import os
import pathlib
import shutil
import subprocess  # noqa: S404 - wrappers around external tools
import sys
import time
from typing import Optional

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
DEFAULT_VO_MATRIX_FILE = REPO_ROOT / 'etc/docker/test/matrix_policy_package_tests.yml'
DEFAULT_RUCIO_CFG_FILE = REPO_ROOT / 'etc/rucio.cfg'
VO_PATH_KEYWORDS = {
    pathlib.Path('rucio_tests'): pathlib.Path('tests'),
    pathlib.Path('rucio_root'): pathlib.Path('/opt/rucio'),
}


def run(*args, check=True, env=None, cwd: Optional[pathlib.Path] = None) -> None:
    kwargs = {'check': check, 'stdout': sys.stderr, 'stderr': subprocess.STDOUT}
    if env is not None:
        kwargs['env'] = env
    if cwd is not None:
        kwargs['cwd'] = str(cwd)
    args = [str(arg) for arg in args]
    print("** Running", " ".join(map(lambda arg: repr(arg) if ' ' in arg else arg, args)), kwargs, file=sys.stderr, flush=True)
    subprocess.run(args, **kwargs)


def load_yaml_file_with_ruby(path: pathlib.Path) -> dict:
    ruby = shutil.which('ruby')
    if not ruby:
        raise RuntimeError("Could not parse YAML: neither PyYAML nor Ruby are available.")

    parser = (
        'require "json"; '
        'require "yaml"; '
        'print JSON.generate(YAML.load(ARGF.read) || {})'
    )
    try:
        proc = subprocess.run(
            [ruby, '-e', parser, str(path)],
            capture_output=True,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError as error:
        stderr = error.stderr.strip()
        details = f" ({stderr})" if stderr else ''
        raise RuntimeError(f"Could not parse YAML file '{path}' via Ruby fallback{details}.") from error

    try:
        loaded = json.loads(proc.stdout)
    except json.JSONDecodeError as error:
        raise RuntimeError(f"Ruby fallback produced invalid JSON for '{path}'.") from error

    if isinstance(loaded, dict):
        return loaded
    raise RuntimeError(f"YAML file '{path}' did not parse to a mapping.")


def load_yaml_file(path: pathlib.Path) -> dict:
    try:
        import yaml  # noqa: PLC0415 -- imported lazily to avoid hard dependency in non-YAML paths
    except ImportError:
        return load_yaml_file_with_ruby(path)

    try:
        with path.open('r') as stream:
            loaded = yaml.safe_load(stream)
    except OSError as error:
        raise RuntimeError(f"Could not read YAML file '{path}'.") from error
    except yaml.YAMLError as error:
        raise RuntimeError(f"Invalid YAML syntax in '{path}'.") from error
    return dict(loaded or {})


def get_vo_section(vo_config: dict, vo: str, section: str):
    if vo not in vo_config:
        raise RuntimeError(f"VO '{vo}' is not defined in the provided VO matrix.")
    if section not in vo_config[vo]:
        raise RuntimeError(f"VO '{vo}' does not define section '{section}'.")
    return vo_config[vo][section]


def get_vo_installation_cmd(vo_config: dict, vo: str) -> str:
    install_cmd = get_vo_section(vo_config, vo, 'installation_cmd')
    if not install_cmd:
        raise RuntimeError(f"No installation command configured for VO '{vo}'.")
    return install_cmd


def persist_vo_config_overrides(vo_config: dict, vo: str, rucio_cfg_file: pathlib.Path) -> dict:
    config_overrides = get_vo_section(vo_config, vo, 'config_overrides')
    if not config_overrides:
        raise RuntimeError(f"No config_overrides configured for VO '{vo}'.")
    if not rucio_cfg_file.is_file():
        raise RuntimeError(f"Rucio config file not found at '{rucio_cfg_file}'.")

    parser = configparser.ConfigParser()
    parser.read(rucio_cfg_file)
    if 'policy' not in parser:
        parser.add_section('policy')
    parser['policy'].clear()
    for key, value in config_overrides.items():
        parser['policy'][key] = value

    with rucio_cfg_file.open('w') as config_stream:
        parser.write(config_stream)
    return config_overrides


def resolve_vo_path(path_expression: str) -> pathlib.Path:
    parts = [VO_PATH_KEYWORDS[pathlib.Path('rucio_root')], *(pathlib.Path(part) for part in pathlib.Path(path_expression).parts)]
    return functools.reduce(
        lambda path, part: path / VO_PATH_KEYWORDS[part] if part in VO_PATH_KEYWORDS else path / part,
        parts,
    )


def collect_vo_tests(vo_config: dict, vo: str) -> str:
    tests_section = get_vo_section(vo_config, vo, 'tests')
    allowed_patterns = tests_section.get('allow', [])
    denied_patterns = tests_section.get('deny', [])

    def expand_paths(patterns: list[str]) -> set[str]:
        expanded: set[str] = set()
        for pattern in patterns:
            resolved = resolve_vo_path(pattern)
            paths = glob.glob(f"{resolved}/test_*.py") if resolved.is_dir() else [str(resolved)]
            expanded.update(path for path in paths if pathlib.Path(path).is_file())
        return expanded

    selected_tests = sorted(expand_paths(allowed_patterns) - expand_paths(denied_patterns))
    return " ".join(selected_tests)


def build_vo_matrix(vo_config: dict) -> list[dict]:
    try:
        return [
            {
                'POLICY': policy_package,
                'DIST': dist,
                'RDBMS': rdbms,
                'PYTHON': python_ver,
                'SUITE': 'votest',
                'IMAGE_IDENTIFIER': f'votest-{image_identifier}',
            }
            for policy_package in vo_config
            for dist in vo_config[policy_package]['dists']
            for rdbms in vo_config[policy_package]['rdbms']
            for image_identifier in vo_config[policy_package]['image_identifier']
            for python_ver in vo_config[policy_package]['python']
        ]
    except KeyError as error:
        raise RuntimeError(f"Missing key in VO matrix configuration: {error}") from error


def wait_for_httpd() -> None:
    run(
        'curl',
        '--retry',
        '15',
        '--retry-all-errors',
        '--retry-delay',
        '1',
        '-k',
        'https://localhost/ping',
    )


def wait_for_database(timeout_seconds: int = 60) -> None:
    deadline = time.time() + timeout_seconds
    wait_command = [sys.executable, '-c', 'from rucio.db.sqla.session import wait_for_database; wait_for_database()']

    while True:
        proc = subprocess.run(wait_command, check=False, stdout=sys.stderr, stderr=subprocess.STDOUT)
        if proc.returncode == 0:
            return
        if time.time() >= deadline:
            raise RuntimeError('Cannot access database')
        time.sleep(1)


def run_suite_from_env() -> None:
    suite = os.environ.get('SUITE', '').strip()
    if not suite:
        raise RuntimeError("Missing SUITE environment variable for --run-suite-from-env.")

    source_path = pathlib.Path(os.environ.get('RUCIO_SOURCE_DIR') or '/usr/local/src/rucio')
    cfg_path = source_path / 'etc/docker/test/extra'
    rucio_home = pathlib.Path(os.environ.get('RUCIO_HOME') or '/opt/rucio')
    print(f"** Running suite from env: suite={suite}", file=sys.stderr, flush=True)

    if suite == 'client':
        run('tools/run_tests.sh', '-i')
        run('cp', str(cfg_path / 'rucio_client.cfg'), str(rucio_home / 'etc/rucio.cfg'))
        client_env = os.environ.copy()
        client_env['RUCIO_HOME'] = str(source_path)
        run(
            'tools/pytest.sh',
            '-v',
            '--tb=short',
            'tests/test_clients.py',
            'tests/test_bin_rucio.py',
            'tests/test_module_import.py',
            env=client_env,
            cwd=source_path,
        )
        return

    if suite == 'votest':
        policy = os.environ.get('POLICY', '').strip()
        if not policy:
            raise RuntimeError("Missing POLICY environment variable for suite 'votest'.")
        wait_for_database()
        vo_matrix_file = rucio_home / 'etc/docker/test/matrix_policy_package_tests.yml'
        vo_cfg_file = rucio_home / 'etc/rucio.cfg'
        vo_config = load_yaml_file(vo_matrix_file)
        persist_vo_config_overrides(vo_config, policy, vo_cfg_file)
        wait_for_httpd()
        run('httpd', '-k', 'restart')

        selected_tests = collect_vo_tests(vo_config, policy)
        suite_env = os.environ.copy()
        if selected_tests:
            suite_env['TESTS'] = selected_tests
        run('tools/run_tests.sh', '-p', env=suite_env)
        return

    if suite == 'multi_vo':
        vo1_home = pathlib.Path(rucio_home)
        vo2_home = vo1_home.parent / 'ts2'
        vo1_home.joinpath('etc').mkdir(parents=True, exist_ok=True)
        vo2_home.joinpath('etc').mkdir(parents=True, exist_ok=True)

        run(
            sys.executable,
            str(source_path / 'tools/merge_rucio_configs.py'),
            '--use-env',
            '-s',
            str(cfg_path / 'rucio_autotests_common.cfg'),
            str(cfg_path / 'rucio_multi_vo_ts2_postgres14.cfg'),
            '-d',
            str(vo2_home / 'etc/rucio.cfg'),
        )
        run(
            sys.executable,
            str(source_path / 'tools/merge_rucio_configs.py'),
            '--use-env',
            '-s',
            str(cfg_path / 'rucio_autotests_common.cfg'),
            str(cfg_path / 'rucio_multi_vo_tst_postgres14.cfg'),
            '-d',
            str(vo1_home / 'etc/rucio.cfg'),
        )

        vo1_alembic_cfg = vo1_home / 'etc/alembic.ini'
        if not vo1_alembic_cfg.exists():
            default_alembic_cfg = pathlib.Path('/opt/rucio/etc/alembic.ini')
            if default_alembic_cfg.exists():
                shutil.copy(default_alembic_cfg, vo1_alembic_cfg)
            else:
                template_alembic_cfg = cfg_path / 'alembic_postgres14.ini'
                if not template_alembic_cfg.exists():
                    raise RuntimeError(f"Missing alembic template file at '{template_alembic_cfg}'.")
                shutil.copy(template_alembic_cfg, vo1_alembic_cfg)

        wait_for_database()
        wait_for_httpd()
        run('httpd', '-k', 'restart')
        run('tools/run_tests.sh', '-m')
        return

    if suite in {'remote_dbs', 'sqlite'}:
        wait_for_database()
        wait_for_httpd()
        if os.environ.get('TESTS', '').strip():
            run('tools/run_tests.sh', '-p')
        else:
            run('tools/run_tests.sh')
        return

    raise RuntimeError(f"Unsupported SUITE '{suite}' for --run-suite-from-env.")


def parse_cli_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Suite-only runner for container-internal execution.')
    parser.add_argument('--print-vo-matrix', action='store_true', help='Print VO matrix JSON from --vo-matrix-file.')
    parser.add_argument('--vo-matrix-file', type=pathlib.Path, default=DEFAULT_VO_MATRIX_FILE, help='Input VO matrix YAML file.')
    parser.add_argument('--vo', default='', help='VO name for VO helper operations.')
    parser.add_argument('--vo-config', action='store_true', help='Apply VO policy config_overrides to --rucio-cfg.')
    parser.add_argument('--vo-tests', action='store_true', help='Print resolved VO test paths.')
    parser.add_argument('--vo-installation-command', action='store_true', help='Print VO installation command.')
    parser.add_argument('--rucio-cfg', type=pathlib.Path, default=DEFAULT_RUCIO_CFG_FILE, help='Target rucio.cfg for --vo-config.')
    parser.add_argument('--run-suite-from-env', action='store_true', help='Run one suite using SUITE/POLICY/TESTS environment variables.')
    return parser.parse_args()


def main() -> None:
    args = parse_cli_args()

    if args.print_vo_matrix:
        print(json.dumps(build_vo_matrix(load_yaml_file(args.vo_matrix_file))))
        return

    if args.vo_config or args.vo_tests or args.vo_installation_command:
        if not args.vo:
            raise RuntimeError("--vo is required for VO helper operations.")
        vo_matrix = load_yaml_file(args.vo_matrix_file)
        if args.vo_config:
            print(persist_vo_config_overrides(vo_matrix, args.vo, args.rucio_cfg))
            return
        if args.vo_tests:
            print(collect_vo_tests(vo_matrix, args.vo))
            return
        print(get_vo_installation_cmd(vo_matrix, args.vo))
        return

    if args.run_suite_from_env:
        run_suite_from_env()
        return
    raise RuntimeError("No execution command specified. Use --run-suite-from-env or VO helper flags.")


if __name__ == '__main__':
    main()
