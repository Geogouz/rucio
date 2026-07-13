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

import argparse
import hashlib
import json
import platform
import subprocess  # noqa: S404
import sys
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence


WEBUI_ACCOUNT = "rucio"
WEBUI_DATASET = "rucio-dataset"
WEBUI_EMAIL = "rucio-dev@cern.ch"
WEBUI_IDENTITY = "rucio"
WEBUI_PASSWORD = "rucio"
WEBUI_RSE = "MOCK"
WEBUI_SCOPE = "user.rucio"

DEPENDENCY_STATE = ".rucio-toolbox-dependencies.json"


def checkout_root(path: Path) -> Path:
    candidate = path.expanduser().resolve()
    if not candidate.is_dir():
        raise RuntimeError(f"WebUI checkout does not exist: {candidate}")

    result = subprocess.run(  # noqa: S603
        ("git", "-C", str(candidate), "rev-parse", "--show-toplevel"),  # noqa: S607
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode:
        raise RuntimeError(f"WebUI path is not a valid Git worktree: {candidate}")
    root = Path(result.stdout.strip()).resolve()

    package_path = root / "package.json"
    try:
        package = json.loads(package_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise RuntimeError(f"Cannot read WebUI package metadata: {package_path}") from error
    if not isinstance(package, dict):
        raise RuntimeError(f"Git worktree is not a Rucio WebUI checkout: {root}")
    scripts = package.get("scripts")
    if package.get("name") != "rucio-webui" or not isinstance(scripts, dict) or "dev" not in scripts:
        raise RuntimeError(f"Git worktree is not a Rucio WebUI checkout: {root}")
    return root


def _executable_version(executable: str) -> str:
    return subprocess.run(  # noqa: S603
        (executable, "--version"),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _dependency_fingerprint(root: Path, node_version: str, npm_version: str) -> str:
    digest = hashlib.sha256()
    for path in (root / "package.json", root / "package-lock.json"):
        try:
            content = path.read_bytes()
        except OSError as error:
            raise RuntimeError(f"Cannot read WebUI dependency metadata: {path}") from error
        digest.update(path.name.encode())
        digest.update(content)
    digest.update(f"{sys.platform}\0{platform.machine()}\0{node_version}\0{npm_version}".encode())
    return digest.hexdigest()


def ensure_dependencies(root: Path, node: str, npm: str) -> None:
    lockfile = root / "package-lock.json"
    if not lockfile.is_file():
        raise RuntimeError(f"Rucio WebUI checkout has no package-lock.json: {root}")

    node_version = _executable_version(node)
    npm_version = _executable_version(npm)
    fingerprint = _dependency_fingerprint(root, node_version, npm_version)
    next_executable = root / "node_modules/.bin/next"
    state_path = root / "node_modules" / DEPENDENCY_STATE
    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        state = {}
    if next_executable.is_file() and isinstance(state, dict) and state.get("fingerprint") == fingerprint:
        print("Rucio WebUI dependencies are current.", flush=True)
        return

    print(f"Installing Rucio WebUI dependencies in {root}.", flush=True)
    subprocess.run((npm, "ci"), cwd=root, check=True)  # noqa: S603
    if not next_executable.is_file():
        raise RuntimeError(f"Rucio WebUI dependency installation did not provide Next.js: {root}")
    state_path.write_text(
        json.dumps({
            "fingerprint": fingerprint,
            "node": node_version,
            "npm": npm_version,
        }, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def process_environment(base: Mapping[str, str], secret: str) -> dict[str, str]:
    environment = dict(base)
    environment.update({
        "AUTH_SECRET": secret,
        "ENABLE_USERPASS_LOGIN": "true",
        "MULTIVO_ENABLED": "false",
        "NEXTAUTH_SECRET": secret,
        "NEXTAUTH_URL": "http://localhost:3000",
        "NEXT_PUBLIC_WEBUI_HOST": "http://localhost:3000",
        "NODE_TLS_REJECT_UNAUTHORIZED": "1",
        "OIDC_ENABLED": "false",
        "PARAMS_ENCODING_ENABLED": "false",
        "PROJECT_URL": "https://rucio.cern.ch/",
        "RUCIO_AUTH_HOST": "http://127.0.0.1:8080",
        "RUCIO_HOST": "http://127.0.0.1:8080",
        "X509_ENABLED": "false",
    })
    return environment


def seed_catalogue() -> None:
    from rucio.common.exception import AccountNotFound, DataIdentifierNotFound, IdentityError, IdentityNotFound
    from rucio.common.types import InternalAccount, InternalScope
    from rucio.core.account import (
        add_account,
        add_account_attribute,
        del_account_attribute,
        get_account,
        list_account_attributes,
        update_account,
    )
    from rucio.core.account_limit import set_local_account_limit
    from rucio.core.did import add_did, get_did
    from rucio.core.identity import add_account_identity, exist_identity_account, verify_identity
    from rucio.core.rse import get_rse_id
    from rucio.core.scope import add_scope, get_scopes
    from rucio.db.sqla.constants import AccountStatus, AccountType, DatabaseOperationType, DIDType, IdentityType
    from rucio.db.sqla.session import db_session

    account = InternalAccount(WEBUI_ACCOUNT)
    scope = InternalScope(WEBUI_SCOPE)
    with db_session(DatabaseOperationType.WRITE) as session:
        try:
            stored_account = get_account(account, session=session)
        except AccountNotFound:
            add_account(account, AccountType.USER, WEBUI_EMAIL, session=session)
        else:
            if stored_account.status != AccountStatus.ACTIVE:
                update_account(account, "status", AccountStatus.ACTIVE, session=session)

        attributes = {item["key"]: item["value"] for item in list_account_attributes(account, session=session)}
        if attributes.get("admin") is not True:
            if "admin" in attributes:
                del_account_attribute(account, "admin", session=session)
            add_account_attribute(account, "admin", True, session=session)

        try:
            verify_identity(WEBUI_IDENTITY, IdentityType.USERPASS, WEBUI_PASSWORD, session=session)
        except IdentityNotFound as error:
            raise RuntimeError(
                f"Existing WebUI identity {WEBUI_IDENTITY!r} has a different password; reset the toolbox catalogue"
            ) from error
        except IdentityError:
            add_account_identity(
                WEBUI_IDENTITY,
                IdentityType.USERPASS,
                account,
                WEBUI_EMAIL,
                default=True,
                password=WEBUI_PASSWORD,
                session=session,
            )
        else:
            if not exist_identity_account(WEBUI_IDENTITY, IdentityType.USERPASS, account, session=session):
                add_account_identity(
                    WEBUI_IDENTITY,
                    IdentityType.USERPASS,
                    account,
                    WEBUI_EMAIL,
                    default=True,
                    session=session,
                )

        if scope not in get_scopes(account, session=session):
            add_scope(scope, account, session=session)

        try:
            get_did(scope, WEBUI_DATASET, session=session)
        except DataIdentifierNotFound:
            add_did(scope, WEBUI_DATASET, DIDType.DATASET, account, session=session)

        set_local_account_limit(account, get_rse_id(WEBUI_RSE, session=session), -1, session=session)


def main(arguments: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Rucio WebUI development support")
    parser.add_argument("command", choices=("seed",))
    options = parser.parse_args(arguments)
    if options.command == "seed":
        seed_catalogue()
        print("Rucio WebUI catalogue fixtures are ready.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
