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

import re
import shutil
import subprocess  # noqa: S404 - workflow utility invokes git
from os import environ as env
from typing import Optional

SEMVER_TAG = re.compile(r'^(\d+)\.(\d+)\.(\d+)$')
RELEASE_BRANCH = re.compile(r'^release[-_/]?(\d+)(?:\D.*)?$')
DEFAULT_RUCIO_TAG = 'latest'


def set_output(name: str, value: str) -> None:
    with open(env['GITHUB_OUTPUT'], 'a') as fh:
        print(f'{name}={value}', file=fh)


def target_ref() -> str:
    if env.get('GITHUB_EVENT_NAME') == 'pull_request':
        base_ref = env.get('GITHUB_BASE_REF', '').strip()
        if base_ref:
            return base_ref
    return env.get('GITHUB_REF_NAME', 'master').strip() or 'master'


def parse_semver(tag: str) -> Optional[tuple[int, int, int]]:
    match = SEMVER_TAG.fullmatch(tag.strip())
    if not match:
        return None
    return tuple(int(part) for part in match.groups())


def latest_semver_for_major(major: int) -> str:
    git = shutil.which('git')
    if not git:
        return ''
    proc = subprocess.run(
        [git, 'tag', '-l', f'{major}.*'],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        return ''
    tags: list[tuple[tuple[int, int, int], str]] = []
    for line in proc.stdout.splitlines():
        tag = line.strip()
        version = parse_semver(tag)
        if version and version[0] == major:
            tags.append((version, tag))
    if not tags:
        return ''
    return sorted(tags)[-1][1]


def resolve_rucio_tag() -> str:
    override = env.get('RUCIO_TAG', '').strip()
    if override:
        return override

    ref = target_ref()
    if ref in {'master', 'main'}:
        return DEFAULT_RUCIO_TAG

    if parse_semver(ref):
        return ref

    release_match = RELEASE_BRANCH.fullmatch(ref)
    if release_match:
        release_tag = latest_semver_for_major(int(release_match.group(1)))
        if release_tag:
            return release_tag
    return DEFAULT_RUCIO_TAG


def main() -> None:
    set_output('target_ref', target_ref())
    set_output('rucio_tag', resolve_rucio_tag())


if __name__ == '__main__':
    main()
