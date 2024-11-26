# /home/gouz/rucio/setuputil.py # TODO[DX]: Debug
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

import subprocess
import sys
from typing import TYPE_CHECKING, Union

from pkg_resources import Requirement, parse_requirements

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

clients_requirements_table = {
    'install_requires': [
        'requests',
        'urllib3',
        'dogpile-cache',
        'tabulate',
        'jsonschema',
        'dataclasses',
    ],
    'ssh': ['paramiko'],
    'kerberos': [
        'kerberos',
        'pykerberos',
        'requests-kerberos',
    ],
    'swift': ['python-swiftclient'],
    'argcomplete': ['argcomplete'],
    'sftp': ['paramiko'],
    'dumper': [
        'python-magic',
    ],
}

dev_requirements = [
    'pytest',
    'pytest-xdist',
    'pytest-cov',
    'pyflakes',
    'flake8',
    'pylint',
    'isort',
    'xmltodict',
    'pytz',
    'pycodestyle',
    'pydoc-markdown',
    'docspec_python',
    'sh',
    'PyYAML',
]

server_requirements_table = {
    'install_requires': clients_requirements_table['install_requires'] + [
        'argcomplete',
        'boto',
        'python-magic',
        'paramiko',
        'boto3',
        'sqlalchemy',
        'alembic',
        'pymemcache',
        'python-dateutil',
        'stomp-py',
        'statsd',
        'geoip2',
        'google-auth',
        'redis',
        'flask',
        'oic',
        'prometheus_client',
    ],
    'oracle': ['cx_oracle'],
    'mongo': ['pymongo'],
    'postgresql': ['psycopg[binary,pool]'],
    'mysql': ['PyMySQL'],
    'kerberos': [
        'kerberos',
        'pykerberos',
        'requests-kerberos',
    ],
    'globus': [
        'PyYAML',
        'globus-sdk',
    ],
    'saml': ['python3-saml'],
    'dev': dev_requirements
}


def run_shell_command(cmd: str) -> Union[str, bytearray, memoryview]:
    """
    Run a shell command in path and return output"

    :param cmd: the shell command.
    :return: Output of the shell command.
    """
    output = subprocess.Popen(["/bin/sh", "-c", cmd], stdout=subprocess.PIPE)
    stdout = output.communicate()[0].strip()
    if isinstance(stdout, bytes):
        stdout = stdout.decode(errors='replace')
    return stdout


def get_rucio_version() -> str:
    python_executable = "'" + sys.executable + "'"
    ver = run_shell_command(
        "PYTHONPATH=lib " + python_executable + " -c "
                                                '"from rucio import version; print(version.version_string())"'
    )
    if not ver:
        raise RuntimeError("Could not fetch Rucio version")
    return str(ver)


def extract_requirement_with_extras(full_req_str: str) -> tuple[str, set[str]]:
    """
    Extracts the base-requirement specification (without any extras) from a full-requirement specification string input.

    :param full_req_str: The full requirement specification (e.g., 'psycopg[binary,pool]').
    :returns: Tuple of (base-requirement specification, set of its extras).
    :raises ValueError: If the input string is invalid and cannot be parsed.
    """

    try:
        req = Requirement.parse(full_req_str)
        return req.key, set(req.extras)
    except:
        raise ValueError(f"Invalid dependency string: {full_req_str}")


# TODO[DX]: Add version checks too?
def build_requirements_table_by_key(
        requirements_table: "Mapping[str, Iterable[str]]"
) -> tuple[dict[str, list[tuple[str, set[str]]]], dict[str, list[str]]]:
    """
    Build lookup tables for requirements while preserving extras information.

    :param requirements_table: A mapping where:
        - Keys are feature groups (e.g., 'ssh', 'kerberos') or 'install_requires'.
        - Values are iterables of full requirement specifications, potentially including extras (e.g., 'psycopg[pool]').

    :returns: A tuple containing two dictionaries:
        - base_req_to_info: Maps base requirements (e.g., 'psycopg') to a list of tuples containing:
            1) The feature group associated with the requirement.
            2) A set of extras (e.g., {'binary'}).
        - extras_require: Maps feature groups to their respective lists of requirement specifications .
    """
    base_req_to_info: dict[str, list[tuple[str, set[str]]]] = {}
    extras_require: dict[str, list[str]] = {}

    for feature_group in requirements_table.keys():
        if feature_group != 'install_requires':
            extras_require[feature_group] = []

        for full_req in requirements_table[feature_group]:
            base_req, base_req_extras = extract_requirement_with_extras(full_req)

            # An object holding the feature group and the extras (further dependencies) for current base-requirement.
            base_req_info = (feature_group, base_req_extras)

            # Handle first the identified base requirement
            if base_req not in base_req_to_info:
                base_req_to_info[base_req] = [base_req_info]
            else:
                base_req_to_info[base_req].append(base_req_info)

            # Handle also possible extra dependencies
            for extra_req in base_req_extras:
                if extra_req not in base_req_to_info:
                    base_req_to_info[extra_req] = [base_req_info]
                else:
                    base_req_to_info[extra_req].append(base_req_info)

    return base_req_to_info, extras_require


def match_define_requirements(
        app_type: str,
        requirements_table: "Mapping[str, Iterable[str]]"
) -> tuple[list[str], dict[str, list[str]]]:
    """
    Prepare and return the 'install_requires' and 'extras_require' objects that are expected by setuptools.setup().
        Structure example:
        - install_requires = ['requests==2.32.3', 'urllib3==1.26.19', 'sqlalchemy==2.0.31', etc...]
        - extras_require = {'postgresql': ['psycopg[binary,pool]==3.2.3',],'oracle': ['cx_oracle==8.3.0'], etc...}
    """

    install_requires = []

    # Get the requirements as defined in "setuputil.py"
    base_req_to_info, extras_require = build_requirements_table_by_key(requirements_table)

    # Get the requirements as autogenerated by pip-compile (and available inside "requirements.XXX.txt")
    req_file_name = "requirements/requirements.{}.txt".format(app_type)

    with open(req_file_name, 'r') as fhandle:
        for req in parse_requirements(fhandle.readlines()):
            if req.key in base_req_to_info:
                for base_req_info in base_req_to_info[req.key]:
                    feature_group, base_req_extras = base_req_info
                    print("requirement found", feature_group, req, file=sys.stderr)
                    if feature_group == 'install_requires':
                        install_requires.append(str(req))
                    else:
                        extras_require[feature_group].append(str(req))

            else:
                print("requirement unused", req, "(from " + req.key + ")", file=sys.stderr)
        sys.stderr.flush()

    for extra, deps in extras_require.items():
        if not deps:
            raise RuntimeError('Empty extra: {}'.format(extra))

    return install_requires, extras_require


def list_all_requirements(app_type: str, requirements_table: "Mapping[str, Iterable[str]]") -> None:
    req_table_by_key, _ = build_requirements_table_by_key(requirements_table)
    req_file_name = "requirements/requirements.{}.txt".format(app_type)

    with open(req_file_name, 'r') as fhandle:
        for req in parse_requirements(fhandle.readlines()):
            if req.key in req_table_by_key:
                print(str(req))
