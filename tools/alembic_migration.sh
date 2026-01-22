#!/bin/bash
# -*- coding: utf-8 -*-
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
set -euo pipefail

THIS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="${REPO_ROOT:-$(cd "$THIS_DIR/.." && pwd)}"

echo "Optional: check is_old_db before upgrade (if available)"
python3 - <<'PY' || true
try:
    import sys
    from rucio.db.sqla.util import is_old_db
    sys.exit(0 if is_old_db() else 1)
except Exception:
    pass
PY
echo "Upgrading the DB to head (pre-check)"
alembic -c "${ALEMBIC_INI:-$REPO_ROOT/etc/alembic.ini}" upgrade head
echo "Optional: check is_old_db after upgrade (if available)"
python3 - <<'PY' || true
try:
    import sys
    from rucio.db.sqla.util import is_old_db
    sys.exit(1 if is_old_db() else 0)
except Exception:
    pass
PY

echo "Validating full Alembic history for schema drift"
python3 "${REPO_ROOT}/tools/check_alembic_history.py" \
  --alembic-cfg "${ALEMBIC_INI:-$REPO_ROOT/etc/alembic.ini}" \
  --models "${MODELS_EXPR:-rucio.db.sqla.models:BASE}" \
  --report-json "${ARTIFACTS_DIR:-$REPO_ROOT/artifacts}/alembic_history_report.json"
