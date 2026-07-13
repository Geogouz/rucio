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

from pathlib import Path

WORKFLOW = Path(__file__).resolve().parents[2] / ".github/workflows/runtime_images.yml"
CLEANUP_WORKFLOW = WORKFLOW.with_name("cleanup_runtime_images.yml")


def test_runtime_hash_covers_every_local_dockerfile_copy() -> None:
    workflow = WORKFLOW.read_text()

    for path in (
        ".dockerignore",
        "etc/docker/dev/rucio/entrypoint.sh",
        "etc/docker/test/extra/00-mpm.conf",
        "etc/docker/test/extra/httpd.conf",
        "etc/docker/test/extra/rucio.conf",
        "etc/certs/hostcert_rucio.key.pem",
        "etc/certs/hostcert_rucio.pem",
        "etc/certs/rucio_ca.pem",
        "etc/certs/ruciouser.key.pem",
        "etc/certs/ruciouser.pem",
        "requirements",
    ):
        assert path in workflow


def test_pull_requests_never_publish_missing_images() -> None:
    workflow = WORKFLOW.read_text()

    assert 'if [[ "${{ github.event_name }}" == "pull_request" ]]' in workflow


def test_runtime_cleanup_has_package_write_permission() -> None:
    workflow = CLEANUP_WORKFLOW.read_text()

    assert "packages: write" in workflow


def test_runtime_cleanup_uses_organization_package_api() -> None:
    workflow = CLEANUP_WORKFLOW.read_text()

    assert "/orgs/${{ github.repository_owner }}/packages/" in workflow
    assert "/users/" not in workflow


def test_runtime_cleanup_is_limited_to_upstream() -> None:
    workflow = CLEANUP_WORKFLOW.read_text()

    assert "github.repository_owner == 'rucio'" in workflow


def test_runtime_cleanup_paginates_package_versions() -> None:
    workflow = CLEANUP_WORKFLOW.read_text()

    assert "gh api --paginate --slurp" in workflow


def test_runtime_cleanup_propagates_deletion_failures() -> None:
    workflow = CLEANUP_WORKFLOW.read_text()

    assert "gh api --method DELETE --silent" in workflow
