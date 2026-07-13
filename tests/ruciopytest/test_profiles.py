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

import pytest

from .profiles import SUITE_DEFINITIONS, get_case, iter_cases, resolve_profile


def test_case_registry_covers_current_ci() -> None:
    cases = list(iter_cases())

    assert len(cases) == 19
    assert len(list(iter_cases("unit"))) == 4
    assert len(list(iter_cases("autotest"))) == 12
    assert len(list(iter_cases("votest"))) == 2
    assert len(list(iter_cases("integration"))) == 1
    assert len({case.id for case in cases}) == len(cases)


def test_regular_case_axes_match_current_ci() -> None:
    actual = {
        (case.suite, case.python, case.rdbms)
        for case in iter_cases("autotest")
    }
    expected = {
        (suite, python, rdbms)
        for python in ("3.9", "3.10")
        for suite, rdbms in (
            ("client", "postgres14"),
            ("remote_dbs", "oracle"),
            ("remote_dbs", "mysql8"),
            ("remote_dbs", "postgres14"),
            ("sqlite", "sqlite"),
            ("multi_vo", "postgres14"),
        )
    }

    assert actual == expected


def test_case_ids_include_every_axis() -> None:
    assert get_case("remote-dbs-py310-oracle").rdbms == "oracle"
    assert get_case("votest-py39-postgres14-atlas").policy == "atlas"

    with pytest.raises(ValueError, match="Unknown test case"):
        get_case("remote-dbs")


def test_profiles_derive_compose_profiles_from_cases() -> None:
    assert resolve_profile("remote_dbs", "mysql8").compose_profiles == ("mysql8",)
    assert resolve_profile("sqlite").compose_profiles == ()
    assert resolve_profile("integration").compose_profiles == (
        "postgres14",
        "storage",
        "externalmetadata",
        "iam",
    )


def test_every_selector_exists() -> None:
    for definition in SUITE_DEFINITIONS.values():
        for selector in definition.test_paths:
            path = selector.split("::", 1)[0]
            assert Path(path).exists(), (
                f"{definition.name} references missing selector {selector}"
            )
