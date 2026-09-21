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

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Generator

    from _pytest.nodes import Item
    from _pytest.reports import TestReport
    from _pytest.runner import CallInfo


JUNIT_OUTCOME_PROPERTY = "rucio.test.outcome"


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item: "Item", call: "CallInfo[None]") -> "Generator[None, None, None]":
    outcome = yield
    report: TestReport = outcome.get_result()
    if report.when == "call" and report.passed and hasattr(report, "wasxfail"):
        item.user_properties.append((JUNIT_OUTCOME_PROPERTY, "xpassed"))
