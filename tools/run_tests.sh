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
IFS=$'\n\t'

selection=false
init_only=false
activate_rse=false
multi_vo=false
trace=false
coverage=false
stop_on_failure=false
iterations=1

function usage {
  echo "Usage: $0 [OPTION]..."
  echo 'Run the simplified local test bootstrap and pytest flow.'
  echo ''
  echo '  -h    Show usage'
  echo '  -2    Run tests twice'
  echo '  -p    Select tests from TESTS environment variable'
  echo '  -i    Only initialize test state'
  echo '  -r    Activate default RSEs after bootstrap'
  echo '  -m    Run multi-VO suite (tst then ts2)'
  echo '  -t    Verbose pytest output'
  echo '  -x    Exit on first test failure'
  echo '  -c    Generate coverage'
}

function ensure_memcached {
  memcached -u root -d >/dev/null 2>&1 || true
}

function flush_memcached {
  ensure_memcached
  if timeout 1 bash -c "cat < /dev/null > /dev/tcp/127.0.0.1/11211" 2>/dev/null; then
    echo 'Clearing memcache'
    echo flush_all > /dev/tcp/127.0.0.1/11211
  else
    echo 'Warning: memcached on port 11211 is not reachable; skipping flush'
  fi
}

function bootstrap_and_sync {
  echo 'Bootstrapping tests'
  tools/bootstrap_tests.py

  echo 'Sync rse_repository'
  tools/sync_rses.py

  echo 'Sync metadata keys'
  tools/sync_meta.py
}

function cleanup_runtime_state {
  echo 'Cleaning old authentication tokens'
  rm -rf /tmp/.rucio_*/

  echo 'Cleaning local RSE directories'
  rm -rf /tmp/rucio_rse/*

  echo 'Removing old SQLite database'
  rm -f /tmp/rucio.db
}

function reset_database_state {
  echo 'Resetting database tables'
  tools/reset_database.py

  if [[ -f /tmp/rucio.db ]]; then
    chmod 666 /tmp/rucio.db
  fi

  echo 'Running full alembic migration'
  ALEMBIC_CONFIG="$RUCIO_HOME/etc/alembic.ini" tools/alembic_migration.sh
}

function run_pytest_once {
  local -a pytest_args=()
  local -a selected_tests=()

  if [[ "$trace" == "true" ]]; then
    pytest_args+=(-vvv)
  else
    pytest_args+=(-v --tb=short)
  fi

  if [[ "$stop_on_failure" == "true" ]]; then
    pytest_args+=(--exitfirst)
  fi

  if [[ "$coverage" == "true" ]]; then
    pytest_args+=(--cov-report term --cov-report xml:.coverage)
  fi

  if [[ "$selection" == "true" && -n "${TESTS:-}" ]]; then
    local default_ifs="$IFS"
    IFS=' '
    read -r -a selected_tests <<< "${TESTS}"
    IFS="$default_ifs"
    echo "Running selected tests:"
    printf '%s\n' "${selected_tests[@]}"
  fi

  tools/pytest.sh "${pytest_args[@]}" "${selected_tests[@]}"
}

function run_pytest_iterations {
  local i
  for i in $(seq 1 "$iterations"); do
    echo "Running test iteration $i"
    run_pytest_once
  done
}

function prepare_single_vo {
  flush_memcached
  cleanup_runtime_state
  reset_database_state

  echo 'Graceful restart of Apache'
  httpd -k graceful

  bootstrap_and_sync

  if [[ "$activate_rse" == "true" ]]; then
    echo 'Activating default RSEs (XRD1, XRD2, XRD3, SSH1)'
    tools/docker_activate_rses.sh
  fi
}

function run_multi_vo_flow {
  export RUCIO_HOME=/opt/rucio/etc/multi_vo/tst
  prepare_single_vo

  if [[ "$init_only" == "true" ]]; then
    return 0
  fi

  echo 'Running tests on VO "tst"'
  run_pytest_iterations

  echo 'Preparing second VO'
  export RUCIO_HOME=/opt/rucio/etc/multi_vo/ts2
  flush_memcached
  bootstrap_and_sync

  if [[ "$activate_rse" == "true" ]]; then
    echo 'Activating default RSEs (XRD1, XRD2, XRD3, SSH1)'
    tools/docker_activate_rses.sh
  fi

  echo 'Running tests on VO "ts2"'
  run_pytest_iterations
}

while getopts "h2pirmxtcklaus" opt; do
  case "$opt" in
    h) usage; exit 0 ;;
    2) iterations=2 ;;
    p) selection=true ;;
    i) init_only=true ;;
    r) activate_rse=true ;;
    m) multi_vo=true ;;
    x) stop_on_failure=true ;;
    t) trace=true ;;
    c) coverage=true ;;
    k|l|a|u|s)
      echo "Option -$opt has been removed from the simplified test harness."
      echo "Use explicit test commands or tools/test/run_tests.py CI target/mode options instead."
      exit 2
      ;;
    *) usage; exit 1 ;;
  esac
done

RUCIO_HOME="${RUCIO_HOME:-/opt/rucio}"

if [[ "$multi_vo" == "true" ]]; then
  run_multi_vo_flow
  exit $?
fi

prepare_single_vo

if [[ "$init_only" == "true" ]]; then
  exit 0
fi

run_pytest_iterations
