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

import builtins
import json
import subprocess  # noqa: S404
from contextlib import contextmanager
from types import SimpleNamespace
from typing import TYPE_CHECKING

import pytest

from tools.devenv import webui

if TYPE_CHECKING:
    import pathlib


def _webui_checkout(path: pathlib.Path) -> None:
    (path / "package.json").write_text(json.dumps({"name": "rucio-webui", "scripts": {"dev": "next dev"}}))
    (path / "package-lock.json").write_text(json.dumps({"lockfileVersion": 3}))


def test_existing_webui_worktree_is_reused(tmp_path: pathlib.Path, monkeypatch) -> None:
    root = tmp_path / "webui"
    root.mkdir()
    _webui_checkout(root)
    (root / "src").mkdir()
    commands = []

    def run(command, **kwargs):
        commands.append((tuple(command), kwargs))
        return subprocess.CompletedProcess(command, 0, stdout=f"{root}\n", stderr="")

    monkeypatch.setattr(webui.subprocess, "run", run)

    assert webui.checkout_root(root / "src") == root
    assert commands == [
        (
            ("git", "-C", str((root / "src").resolve()), "rev-parse", "--show-toplevel"),
            {"check": False, "capture_output": True, "text": True},
        )
    ]


def test_webui_dependencies_are_installed_only_when_inputs_change(tmp_path: pathlib.Path, monkeypatch) -> None:
    _webui_checkout(tmp_path)
    installations = []

    def run(command, **kwargs):
        if command[-1] == "--version":
            version = "v22.0.0" if command[0] == "/usr/bin/node" else "10.0.0"
            return subprocess.CompletedProcess(command, 0, stdout=f"{version}\n", stderr="")
        installations.append((tuple(command), kwargs))
        (tmp_path / "node_modules/.bin").mkdir(parents=True, exist_ok=True)
        (tmp_path / "node_modules/.bin/next").write_text("next")
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(webui.subprocess, "run", run)

    webui.ensure_dependencies(tmp_path, "/usr/bin/node", "/usr/bin/npm")
    webui.ensure_dependencies(tmp_path, "/usr/bin/node", "/usr/bin/npm")
    (tmp_path / "package-lock.json").write_text(json.dumps({"lockfileVersion": 3, "changed": True}))
    webui.ensure_dependencies(tmp_path, "/usr/bin/node", "/usr/bin/npm")

    assert installations == [
        (("/usr/bin/npm", "ci"), {"cwd": tmp_path, "check": True}),
        (("/usr/bin/npm", "ci"), {"cwd": tmp_path, "check": True}),
    ]
    state = json.loads((tmp_path / "node_modules" / webui.DEPENDENCY_STATE).read_text())
    assert state["node"] == "v22.0.0"
    assert state["npm"] == "10.0.0"


def test_webui_dependency_install_requires_lockfile(tmp_path: pathlib.Path) -> None:
    (tmp_path / "package.json").write_text("{}")

    with pytest.raises(RuntimeError, match="package-lock.json"):
        webui.ensure_dependencies(tmp_path, "/usr/bin/node", "/usr/bin/npm")


def test_webui_uses_conventional_development_credentials() -> None:
    assert (webui.WEBUI_IDENTITY, webui.WEBUI_PASSWORD, webui.WEBUI_ACCOUNT) == ("rucio", "rucio", "rucio")
    assert webui.WEBUI_RSE == "MOCK"


def test_webui_process_environment_overrides_checkout_configuration() -> None:
    environment = webui.process_environment(
        {
            "PATH": "/usr/bin",
            "RUCIO_HOST": "https://production.example",
            "OIDC_ENABLED": "true",
        },
        "development-secret",
    )

    assert environment == {
        "AUTH_SECRET": "development-secret",
        "ENABLE_USERPASS_LOGIN": "true",
        "MULTIVO_ENABLED": "false",
        "NEXTAUTH_SECRET": "development-secret",
        "NEXTAUTH_URL": "http://localhost:3000",
        "NEXT_PUBLIC_WEBUI_HOST": "http://localhost:3000",
        "NODE_TLS_REJECT_UNAUTHORIZED": "1",
        "OIDC_ENABLED": "false",
        "PARAMS_ENCODING_ENABLED": "false",
        "PATH": "/usr/bin",
        "PROJECT_URL": "https://rucio.cern.ch/",
        "RUCIO_AUTH_HOST": "http://127.0.0.1:8080",
        "RUCIO_HOST": "http://127.0.0.1:8080",
        "X509_ENABLED": "false",
    }


def test_existing_webui_catalogue_fixtures_are_preserved(monkeypatch) -> None:
    session = object()

    @contextmanager
    def db_session(operation):
        yield session

    def unexpected_change(*args, **kwargs):
        pytest.fail("existing catalogue fixtures must be reused")

    rses = []
    limits = []
    modules = {
        "rucio.common.exception": SimpleNamespace(
            AccountNotFound=LookupError,
            DataIdentifierNotFound=LookupError,
            IdentityError=RuntimeError,
            IdentityNotFound=LookupError,
        ),
        "rucio.common.types": SimpleNamespace(InternalAccount=str, InternalScope=str),
        "rucio.core.account": SimpleNamespace(
            add_account=unexpected_change,
            add_account_attribute=unexpected_change,
            del_account_attribute=unexpected_change,
            get_account=lambda account, **kwargs: SimpleNamespace(status="active"),
            list_account_attributes=lambda account, **kwargs: [{"key": "admin", "value": True}],
            update_account=unexpected_change,
        ),
        "rucio.core.account_limit": SimpleNamespace(
            set_local_account_limit=lambda *args, **kwargs: limits.append((args, kwargs)),
        ),
        "rucio.core.did": SimpleNamespace(add_did=unexpected_change, get_did=lambda *args, **kwargs: {"type": "DATASET"}),
        "rucio.core.identity": SimpleNamespace(
            add_account_identity=unexpected_change,
            exist_identity_account=lambda *args, **kwargs: True,
            verify_identity=lambda *args, **kwargs: True,
        ),
        "rucio.core.rse": SimpleNamespace(get_rse_id=lambda rse, **kwargs: rses.append(rse) or "mock-id"),
        "rucio.core.scope": SimpleNamespace(add_scope=unexpected_change, get_scopes=lambda *args, **kwargs: [webui.WEBUI_SCOPE]),
        "rucio.db.sqla.constants": SimpleNamespace(
            AccountStatus=SimpleNamespace(ACTIVE="active"),
            AccountType=SimpleNamespace(USER="user"),
            DatabaseOperationType=SimpleNamespace(WRITE="write"),
            DIDType=SimpleNamespace(DATASET="dataset"),
            IdentityType=SimpleNamespace(USERPASS="userpass"),
        ),
        "rucio.db.sqla.session": SimpleNamespace(db_session=db_session),
    }
    real_import = builtins.__import__

    def import_module(name, *args, **kwargs):
        return modules[name] if name in modules else real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", import_module)

    webui.seed_catalogue()

    assert rses == ["MOCK"]
    assert limits[0][0][1:] == ("mock-id", -1)
    assert limits[0][1] == {"session": session}
