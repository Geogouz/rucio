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

# NOTE: Keep this file Bash-3 compatible (macOS /bin/bash). Avoid declare -g, mapfile, etc.

declare -a TEST_COMMANDS
declare -a TEST_DESCRIPTIONS
declare -a TEST_PYTHON_VERSIONS
declare -a TEST_DATABASES
declare -a TEST_SERVICE_SETS
declare -a TEST_IMAGE_IDENTIFIERS
TEST_COMMANDS=()
TEST_DESCRIPTIONS=()
TEST_PYTHON_VERSIONS=()
TEST_DATABASES=()
TEST_SERVICE_SETS=()
TEST_IMAGE_IDENTIFIERS=()

# Track caller selections with conservative defaults.
SELECTED_TEST=""
PYTEST_FILTER=""
REUSE_AUTOTEST_IMAGES=false

# bootstrap_tests::detect_matrix_parser
# ----------------------------------------------
# Return the first matrix_parser.py path discovered in the repository.
#
function bootstrap_tests::detect_matrix_parser() {
  local repo_root parser_candidates candidate

  if [[ -n "${RUCIO_REPO_ROOT:-}" ]]; then
    repo_root="$RUCIO_REPO_ROOT"
  else
    repo_root="$(bootstrap_common::repo_root)"
  fi

  parser_candidates=(
    "$repo_root/tools/test/matrix_parser.py"
    "$repo_root/tools/matrix_parser.py"
    "$repo_root/matrix_parser.py"
  )

  for candidate in "${parser_candidates[@]}"; do
    if [[ -f "$candidate" ]]; then
      echo "$candidate"
      return 0
    fi
  done

  return 1
}

# bootstrap_tests::append_matrix_tests <matrix_file> <description_prefix> <include_matrix_arg> <parser_path>
# ----------------------------------------------
# Append discovered matrix entries to the available test list.
#
function bootstrap_tests::append_matrix_tests() {
  local matrix_file="$1"
  local description_prefix="$2"
  local include_matrix_arg="${3:-false}"
  local parser_path="$4"

  if [[ ! -f "$matrix_file" ]]; then
    bootstrap_common::warn "Test matrix not found: $matrix_file"
    return
  fi

  local matrix_json
  if ! matrix_json=$(python3 "$parser_path" < "$matrix_file" 2>/dev/null); then
    bootstrap_common::warn "Unable to parse test matrix with $parser_path: $matrix_file"
    return
  fi

  local matrix_cases=()
  while IFS= read -r line; do
    matrix_cases+=("$line")
  done < <(echo "$matrix_json" | jq -c '(. // []) | .[]?')

  if ((${#matrix_cases[@]} == 0)); then
    bootstrap_common::warn "Test matrix is empty: $matrix_file"
    return
  fi

  local case dist py suite db services desc base_cmd cmd matrix_quoted
  for case in "${matrix_cases[@]}"; do
    dist=$(echo "$case"    | jq -r '.DIST')
    py=$(echo   "$case"    | jq -r '.PYTHON')
    suite=$(echo "$case"   | jq -r '.SUITE')
    db=$(echo    "$case"   | jq -r '.RDBMS // empty')
    services=$(echo "$case"| jq -r '[.SERVICES] | flatten | join(",")')

    desc="${description_prefix}${suite} (dist ${dist}, py ${py}"
    [[ -n "$db" ]] && desc+=", db ${db}"
    [[ -n "$services" ]] && desc+=", services ${services}"
    desc+=")"

    base_cmd="$RUCIO_REPO_ROOT/tools/run_autotests.sh --build"
    if [[ "$include_matrix_arg" == "true" ]]; then
      matrix_quoted=$(printf '%q' "$matrix_file")
      base_cmd+=" ${matrix_quoted}"
    fi

    cmd="$base_cmd -d ${dist} --py ${py} --suite ${suite}"
    [[ -n "$db" ]] && cmd+=" --db ${db}"

    if [[ "${REUSE_AUTOTEST_IMAGES:-false}" == true ]]; then
      cmd="RUCIO_AUTOTEST_REUSE_IMAGES=1 $cmd"
    fi

    TEST_COMMANDS+=("$cmd")
    TEST_DESCRIPTIONS+=("$desc")
    TEST_PYTHON_VERSIONS+=("$py")
    TEST_DATABASES+=("$db")
    TEST_SERVICE_SETS+=("$services")
    TEST_IMAGE_IDENTIFIERS+=("$(echo "$case" | jq -r '.IMAGE_IDENTIFIER // ""')")
  done
}

# bootstrap_tests::configure <selected_test> <pytest_filter> <reuse_flag>
# ----------------------------------------------
# Store CLI selections for later helpers.
#
function bootstrap_tests::configure() {
  local selected_test="${1:-}"
  local pytest_filter="${2:-}"
  local reuse_images="${3:-false}"

  SELECTED_TEST="$selected_test"
  PYTEST_FILTER="$pytest_filter"
  if [[ "$reuse_images" == "true" ]]; then
    REUSE_AUTOTEST_IMAGES=true
  else
    REUSE_AUTOTEST_IMAGES=false
  fi
}

# bootstrap_tests::gather
# ----------------------------------------------
# Collect the available test commands and descriptions.
#
function bootstrap_tests::gather() {
  TEST_COMMANDS=()
  TEST_DESCRIPTIONS=()
  TEST_PYTHON_VERSIONS=()
  TEST_DATABASES=()
  TEST_SERVICE_SETS=()
  TEST_IMAGE_IDENTIFIERS=()

  if [[ -z "${RUCIO_REPO_ROOT:-}" ]]; then
    RUCIO_REPO_ROOT="$(bootstrap_common::repo_root)"
  fi

  local compose_cmd
  compose_cmd="$(bootstrap_docker::compose_cmd_string)"

  # 1) Default local test suite
  TEST_COMMANDS+=("$compose_cmd --project-name dev exec rucio tools/run_tests.sh")
  TEST_DESCRIPTIONS+=("run local test suite (tools/run_tests.sh)")
  TEST_PYTHON_VERSIONS+=("")
  TEST_DATABASES+=("")
  TEST_SERVICE_SETS+=("")
  TEST_IMAGE_IDENTIFIERS+=("")

  # 2) Matrix entries from YAML via Python helper
  local parser_path
  parser_path="$(bootstrap_tests::detect_matrix_parser)"

  if [[ -z "$parser_path" ]]; then
    bootstrap_common::warn "matrix_parser.py not found"
    return
  fi

  if ! command -v python3 >/dev/null 2>&1; then
    bootstrap_common::warn "python3 not found; skipping matrix discovery"
    return
  fi
  if ! command -v jq >/dev/null 2>&1; then
    bootstrap_common::warn "jq not found; skipping matrix discovery"
    return
  fi

  local matrix_yml
  matrix_yml="$RUCIO_REPO_ROOT/etc/docker/test/matrix.yml"
  bootstrap_tests::append_matrix_tests "$matrix_yml" "autotest: " false "$parser_path"

  local integration_matrix
  integration_matrix="$RUCIO_REPO_ROOT/etc/docker/test/matrix_integration_tests.yml"
  bootstrap_tests::append_matrix_tests "$integration_matrix" "integration workflow: " true "$parser_path"
}

# bootstrap_tests::print_available
# ----------------------------------------------
# Print a numbered list of discovered tests.
#
function bootstrap_tests::print_available() {
  echo
  echo "Available tests:"
  local idx=1
  local desc
  for desc in "${TEST_DESCRIPTIONS[@]}"; do
    printf "  %2d) %s\n" "$idx" "$desc"
    idx=$((idx+1))
  done
  echo "Run with '$0 --test <number>' or '$0 -t <number>' to execute one automatically."
}

# bootstrap_tests::run_selected
# ----------------------------------------------
# Run the requested test, applying optional filters and reuse flags.
#
function bootstrap_tests::run_selected() {
  if [[ -z "${SELECTED_TEST:-}" ]]; then
    return
  fi

  if ! [[ "${SELECTED_TEST:-}" =~ ^[0-9]+$ ]] || (( SELECTED_TEST < 1 || SELECTED_TEST > ${#TEST_COMMANDS[@]} )); then
    bootstrap_common::error "Invalid test number '${SELECTED_TEST:-}'"
    return 1
  fi

  cd "$RUCIO_REPO_ROOT"
  bootstrap_common::info "Running test #${SELECTED_TEST:-}: ${TEST_DESCRIPTIONS[SELECTED_TEST-1]}"
  local cmd filter_escaped
  cmd="${TEST_COMMANDS[SELECTED_TEST-1]}"
  if [[ -n "${PYTEST_FILTER:-}" ]]; then
    filter_escaped=$(printf '%q' "${PYTEST_FILTER:-}")
    if [[ "${SELECTED_TEST:-}" == "1" ]]; then
      cmd="$(bootstrap_docker::compose_cmd_string) --project-name dev exec -e TESTS=$filter_escaped rucio tools/run_tests.sh -p"
    else
      cmd+=" --filter $filter_escaped"
    fi
  fi
  eval "$cmd"
}

# bootstrap_tests::python_for_test <number>
# ----------------------------------------------
# Return the Python version declared for a matrix-based test entry.
# Empty string means no dedicated runtime is needed.
#
function bootstrap_tests::python_for_test() {
  local test_number="$1"

  if [[ -z "$test_number" || ! "$test_number" =~ ^[0-9]+$ ]]; then
    return
  fi

  if (( test_number < 1 || test_number > ${#TEST_PYTHON_VERSIONS[@]} )); then
    return
  fi

  local idx=$((test_number - 1))
  echo "${TEST_PYTHON_VERSIONS[idx]}"
}

# bootstrap_tests::description_for_test <number>
# ----------------------------------------------
# Return the description string for a discovered test.
#
function bootstrap_tests::description_for_test() {
  local test_number="$1"

  if [[ -z "$test_number" || ! "$test_number" =~ ^[0-9]+$ ]]; then
    return
  fi

  if (( test_number < 1 || test_number > ${#TEST_DESCRIPTIONS[@]} )); then
    return
  fi

  local idx=$((test_number - 1))
  echo "${TEST_DESCRIPTIONS[idx]}"
}

# bootstrap_tests::image_identifier_for_test <number>
# ----------------------------------------------
# Return the image identifier string for a discovered test entry.
#
function bootstrap_tests::image_identifier_for_test() {
  local test_number="$1"

  if [[ -z "$test_number" || ! "$test_number" =~ ^[0-9]+$ ]]; then
    return
  fi

  if (( test_number < 1 || test_number > ${#TEST_IMAGE_IDENTIFIERS[@]} )); then
    return
  fi

  local idx=$((test_number - 1))
  echo "${TEST_IMAGE_IDENTIFIERS[idx]}"
}

# bootstrap_tests::collect_profiles_for_test <number> <array_var>
# ----------------------------------------------
# Populate <array_var> with the docker-compose profiles implied by the
# matrix entry: database engines plus optional service bundles.
#
function bootstrap_tests::collect_profiles_for_test() {
  local test_number="$1"
  local target_array="$2"

  eval "${target_array}=()"

  if [[ -z "$test_number" || ! "$test_number" =~ ^[0-9]+$ ]]; then
    return 1
  fi

  if (( test_number < 1 || test_number > ${#TEST_DATABASES[@]} )); then
    return 1
  fi

  local idx=$((test_number - 1))
  local db_profile="${TEST_DATABASES[idx]}"
  local service_string="${TEST_SERVICE_SETS[idx]}"

  local -a computed=()

  # sqlite does not rely on an external compose profile.
  if [[ -n "$db_profile" && "$db_profile" != "sqlite" ]]; then
    computed+=("$db_profile")
  fi

  if [[ -n "$service_string" ]]; then
    local -a services
    IFS=',' read -r -a services <<< "$service_string"
    local svc
    for svc in "${services[@]}"; do
      if [[ -z "$svc" ]]; then
        continue
      fi

      # Trim leading/trailing whitespace.
      while [[ "${svc:0:1}" == " " ]]; do
        svc="${svc:1}"
      done
      while [[ "${svc: -1}" == " " ]]; do
        svc="${svc::-1}"
      done

      local duplicate="false"
      local existing
      # Use the "+" parameter expansion trick to avoid set -u errors when the
      # array is currently empty on older Bash versions (e.g. macOS /bin/bash).
      for existing in "${computed[@]+${computed[@]}}"; do
        if [[ "$existing" == "$svc" ]]; then
          duplicate="true"
          break
        fi
      done
      if [[ "$duplicate" == "true" ]]; then
        continue
      fi

      computed+=("$svc")
    done
  fi

  local idx_out=0
  local value
  # See comment above regarding the use of "${array[@]+${array[@]}}".
  for value in "${computed[@]+${computed[@]}}"; do
    bootstrap_common::array_assign "$target_array" "$idx_out" "$value"
    idx_out=$((idx_out + 1))
  done

  return 0
}
