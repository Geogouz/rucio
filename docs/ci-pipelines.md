# Continuous Integration Pipelines

This document is the **source of truth** for Rucio’s CI. It explains how jobs are triggered, how runtime images and matrices are produced, how tests are orchestrated inside containers (or directly on runners), and which files wire everything together. A complete file inventory is provided at the end.

---

## At a glance

- **Workflows**
  - **Runtime image build** — reusable workflow providing Python runtime images to the rest of CI.
  - **Runtime image cache builder** — refreshes autotest/integration caches on GHCR.
  - **Runtime image cleanup** — prunes aged GHCR artefacts on a schedule or by hand.
  - **VO‑specific tests** — policy‑package suites executed in containers.
  - **End‑to‑End integration tests** — full compose stack, external services, targeted integration suites.
  - **Unit tests** — pure GitHub runner (no containers), coverage published to Codecov.
  - **Run all the autotests** — umbrella workflow: headers, style/quality, and the CI test matrix.

- **Shared building blocks**
  - **Reusable runtime images** built by `.github/workflows/runtime_images.yml` from `etc/docker/test/runtime.Dockerfile` + pinned `requirements/*`.
  - **Matrices** defined under `etc/docker/test/*.yml` and converted to JSON by `tools/test/matrix_parser.py` or constructed by `tools/test/votest_helper.py`.
  - **Image build planners** `tools/test/build_images.py` and `tools/test/donkeyrider.py` expand matrices into Docker build/run actions (the former drives cache refreshes, the latter underpins `tools/run_autotests.sh`).
  - **Orchestrator** `tools/test/run_tests.py` chooses between **single‑container** runs and the **dev docker‑compose** stack.
  - **Config overlays & Alembic**: `etc/docker/test/extra/*` merged or copied by `etc/docker/dev/rucio/entrypoint.sh` (details below).

---

## Reusable runtime images

**Workflow:** `.github/workflows/runtime_images.yml`

- Builds OCI images for **Python 3.9 and 3.10** from `etc/docker/test/runtime.Dockerfile`.
- Image content is **content‑addressed**: a tag is derived from a SHA‑256 over `PYTHON`, the Dockerfile, and the two requirements lockfiles.
- On success it publishes to **GHCR** and exposes three outputs:
  `py39_image`, `py310_image`, and `build_locally` (flagging downstream jobs to build locally when pulling is not possible).
- The job first checks whether the resolved tag already exists on GHCR and skips rebuilding when it does. When the tag is missing **and** the run is a pull request targeting `master`, it flips `build_locally=true` so downstream jobs know they must build the image on the runner because the workflow cannot push to GHCR from a forked PR.
- Exposed as a reusable `workflow_call`, with manual `workflow_dispatch` support alongside the nightly cron, plus push triggers limited to the **`master`** branch when the Dockerfile or lockfiles change.

**Inputs & assets**
- `etc/docker/test/runtime.Dockerfile`
- `requirements/requirements.server.txt`
- `requirements/requirements.dev.txt`

---

## Runtime image cache builder — `.github/workflows/imagecache.yml`

**Triggers:** weekday cron (03:00 UTC) and manual dispatch.

1. **setup** — checks out the tree and asks `tools/github/workflow/grabrelease.py` to enumerate release branches plus `master`.
2. **build_autotests** — iterates over the selected branches, parses `etc/docker/test/matrix.yml` via `tools/test/matrix_parser.py`, and feeds the resulting cases to `tools/test/build_images.py`. Each image is tagged as `ghcr.io/<owner>/<repo>/rucio-autotest:<dist>-python<version>` (for example `.../rucio-autotest:alma9-python3.9`, with an optional `-<release>` suffix when `--branch` targets a release line) and, when building for `master`, pushed back to GHCR via `--push-cache`. Scheduled runs add `--build-no-cache` so the base distribution packages are refreshed. Non‑`master` branches exit early to avoid overwriting shared tags.
3. **build_integration_tests** — repeats the process for `etc/docker/test/matrix_integration_tests.yml`, checking out **`rucio/containers`** (its default branch supplies the Dockerfiles) alongside the selected Rucio branch to provide the source tree. As with the autotest cache, non‑`master` branches exit early, so only `master` publishes images such as `ghcr.io/<owner>/<repo>/rucio-integration-test:alma9-python3.9` for the integration workflow to reuse.

The cache workflow operates purely through GHCR tags and the Docker BuildKit options emitted by `build_images.py`; no tests are executed here.

---

## Runtime image cleanup — `.github/workflows/cleanup_runtime_images.yml`

**Triggers:** cron (Sundays 02:00 UTC for untagged images, first of the month 03:00 UTC for tagged images) and manual dispatch with selectable cleanup mode.

- **cleanup-untagged** — calls the GitHub Container Registry API to delete untagged `rucio-dev-runtime` images older than 7 days.
- **cleanup-tagged** — performs the same sweep for tagged artefacts older than 30 days while explicitly preserving `*-buildcache` tags.

Both jobs authenticate with the Actions token and rely on the GitHub REST API + `jq` filtering to identify candidate image versions.

---

## Orchestration building blocks

### Matrices and parsers

- **General/autotest & integration**
  `etc/docker/test/matrix.yml`, `matrix_nightly.yml`, `matrix_integration_tests.yml`
  parsed by `tools/test/matrix_parser.py`. The parser normalises keys
  `dists, python, suites, image_identifier, services` → `DIST, PYTHON, SUITE, IMAGE_IDENTIFIER, SERVICES`, expands `allow`/`deny` lists, and emits a JSON array consumed by GitHub Actions.

- **VO policy suites**
  `etc/docker/test/matrix_policy_package_tests.yml`
  consumed by `tools/test/votest_helper.py`, which validates VO sections, persists VO‑specific config overrides, and returns a matrix with the correct policy package, distro, Python, DB, and image identifier.

### Development stack & container entrypoint

- **Dev compose**: `etc/docker/dev/docker-compose.yml` runs a `rucio` (httpd) service and a `rucioclient` sidecar. During CI the `rucio` service image is overridden to the **runtime image** selected by the matrix. It also mounts identity provider secrets from `etc/docker/dev/rucio/idpsecrets.json`.
- **Entrypoint**: `etc/docker/dev/rucio/entrypoint.sh` prepares `rucio.cfg` and `alembic.ini` from `etc/docker/test/extra/`:
  - **Default (no `RDBMS`)** — copies `rucio_default.cfg` to `rucio.cfg` and `alembic_default.ini` to `alembic.ini` **without merging**.
  - **DB‑specific branches** — merges `rucio_autotests_common.cfg` with the selected overlay (`rucio_postgres14.cfg`, `rucio_oracle.cfg`, `rucio_mysql8.cfg`, `rucio_sqlite.cfg`) via `tools/merge_rucio_configs.py`, and copies the matching Alembic file.

### Runners and helpers

- **Top‑level orchestrator**: `tools/test/run_tests.py`
  - **RUN_HTTPD=true (default)** → bring up dev compose and `docker compose exec` into `rucio`.
  - **RUN_HTTPD=false** → start a **single ephemeral container** and run `tools/test/test.sh` directly (used by syntax‑only jobs).
  - Concurrency knobs (environment variables): `USE_PODMAN`, `PARALLEL_AUTOTESTS`, `PARALLEL_AUTOTESTS_PROCNUM`, `PARALLEL_AUTOTESTS_FAILFAST`.

- **Shell/Test entrypoints**
  - `syntax` → runs `tools/test/check_syntax.sh` (Ruff) with its defaults (**bin/**, **lib/**, **tools/*.py**). Use `SYNTAX_RUFF_ARGS` to expand or narrow this set.
  - `client_syntax` → runs the same checker but builds `SYNTAX_RUFF_ARGS` via `tools/test/ignoretool.py --ruff` and limits checks to CLI artefacts and client-specific tests.
  - `client` → initialises the stack via `tools/run_tests.sh -i`, applies `rucio_client.cfg`, then runs client tests with `tools/pytest.sh`.
  - `votest` → waits for the services, rewrites the policy section in `rucio.cfg` via `tools/test/votest_helper.py --vo-config`, and then executes the VO-specific test selection returned by the helper.
  - Other suites reuse these helpers as required.
  - `tools/pytest.sh` launches pytest (disables auto-plugins, enables `xdist` except on sqlite/mysql/oracle) and translates `/opt/rucio/tests/...` paths when running with mounted sources.
  - `tools/run_multi_vo_tests_docker.sh` performs full **multi-VO** initialisation and (optionally) RSE activation.

- **DB/bootstrap/alembic & data**
  `tools/reset_database.py`, `tools/alembic_migration.sh`, `tools/bootstrap_tests.py`, `tools/sync_rses.py` (uses `etc/rse_repository.json`), `tools/sync_meta.py`.
  Multi‑VO configs like `etc/docker/test/extra/rucio_multi_vo_tst_postgres14.cfg` reference `/opt/rucio/etc/multi_vo/tst/etc/alembic.ini` inside the container.

---

## Workflows in detail

### VO‑specific tests — `.github/workflows/vo_tests.yml`

**Triggers:** PR, push, manual, nightly.

1. **runtime_images** — resolves image tags via the reusable workflow.
2. **setup** — installs `PyYAML`, writes `branch` to `$GITHUB_OUTPUT` via `tools/github/workflow/set_branch_output_env.py`, and emits a JSON **matrix** by running `tools/test/votest_helper.py`.
3. **test** — logs into GHCR; when `build_locally == 'true'` builds the image inside the job (using cache‑from); then streams a single matrix case into `tools/test/run_tests.py`.

**Suites & policy packages today:** **atlas**, **belleii** (both on AlmaLinux 9, Python 3.9, PostgreSQL 14).

---

### End‑to‑End integration tests — `.github/workflows/integration_tests.yml`

**Triggers:** PR, push, nightly.

1. **runtime_images** — resolves the **Python 3.9** runtime image.
2. **setup** — installs `PyYAML`, computes `branch`, turns `etc/docker/test/matrix_integration_tests.yml` into a JSON matrix via `tools/test/matrix_parser.py`.
3. **integration-tests** — checks out `rucio/containers` and the Rucio source, selects an appropriate containers tag, optionally builds the runtime image locally, pulls service images, **overrides the `rucio` service** to use the runtime image, and brings the stack up with the **storage, externalmetadata, iam** profiles **defined in the workflow**. Once the stack is ready it copies the test templates, installs Rucio in editable mode, runs `tools/run_tests.sh -ir`, and then executes targeted pytest modules that cover uploads/downloads, transfer clients, the protocol backends (gfal2, xrootd, ssh, rsync, rclone), the conveyor, third-party copy verification against the FTS container, token deletion, and the external metadata plugins before tearing the stack down.【F:.github/workflows/integration_tests.yml†L103-L211】

**Notes:** this workflow orchestrates Compose and pytest directly; it **does not** call `tools/test/run_tests.py`.

---

### Unit tests — `.github/workflows/unit_tests.yml`

**Triggers:** PR, push, nightly.

- Runs a **Python matrix** `3.9, 3.10, 3.11, 3.12` on `ubuntu-latest`.
- Installs system deps (Kerberos, XMLSec), upgrades pip tooling, installs `requirements/requirements.dev.txt`, and executes `pytest tests/rucio --cov=lib/rucio`.
- Publishes coverage with `codecov/codecov-action`.

---

### Run all the autotests — `.github/workflows/autotest.yml`

**Triggers:** PR, push, nightly.

Jobs:
1. **Add header lint** — `python3 tools/add_header --dry-run --disable-progress-bar` ensures new/changed files carry the standard header. The job is disabled for scheduled runs in forks (`if: github.repository_owner == 'rucio' || github.event_name != 'schedule'`).
2. **python_ruff** — `ruff check --output-format=github .`.
3. **Check Python Type Annotations** — generates a report with `flake8-annotations` (via `tools/count_missing_type_annotations_utils.sh`), checks out the ancestor of `rucio/master` using the local action `.github/actions/checkout_ancestor_commit`, and fails the job if missing annotations **increased**. This job is skipped for scheduled runs and for release or tag builds.
4. **Python type check (Pyright)** — runs `tools/run_pyright.sh` against the current tree, compares the report with the ancestor of `rucio/master`, and fails on regressions. It follows the same scheduling and release/tag exclusions as the annotation check.
5. **Build Runtime Images** — reuses `.github/workflows/runtime_images.yml` to build and publish the Python 3.9/3.10 runtime images on GHCR whenever the workflow has push permission. On forked pull requests it cannot push, so it only returns the computed tags and flips `build_locally=true` for the downstream jobs. The job is skipped during the scheduled nightly run because the standalone runtime-image workflow already refreshed the tags.【F:.github/workflows/autotest.yml†L138-L145】
6. **Matrix setup & tests** — installs prerequisites, computes the matrix (`etc/docker/test/matrix.yml` for PRs/pushes, or `matrix_nightly.yml` for scheduled runs), and then, for each case, builds the required runtime image locally when `build_locally=='true'` before streaming the matrix entry into `tools/test/run_tests.py`. Suites include `syntax`, `client_syntax`, `client`, `remote_dbs` (PostgreSQL 14, MySQL 8, Oracle), `sqlite`, and `multi_vo`.【F:.github/workflows/autotest.yml†L145-L222】

---

## Container stack details (when `RUN_HTTPD=true`)

- **Image**: runtime image from the matrix (built or pulled).
- **Compose**: `etc/docker/dev/docker-compose.yml` (the `rucio` service is overridden to use the runtime image and the working tree is mounted).
- **Entrypoint**: generates `rucio.cfg` and `alembic.ini` from overlays and DB‑specific templates or copies defaults when `RDBMS` is unset; updates CA trust; symlinks `/usr/bin/python` if needed.
- **Test flow**: suites such as `client`, `remote_dbs`, and `sqlite` rely on `tools/run_tests.sh` to reset databases, bootstrap data, and hand off to `tools/pytest.sh` (`xdist` disabled on sqlite/mysql/oracle). Only the `multi_vo` suite calls `tools/run_multi_vo_tests_docker.sh` before invoking pytest.

---

## Environment variables of interest

- **Matrix payload**: `SUITE`, `PYTHON`, `DIST`, `IMAGE_IDENTIFIER`, `POLICY`, `RDBMS`, and `RUCIO_HOME` are injected via the matrix JSON and consumed by the shell helpers (`tools/test/test.sh`, `tools/run_multi_vo_tests_docker.sh`) to select suites, databases, and configuration overlays.【F:tools/test/test.sh†L21-L111】【F:tools/run_multi_vo_tests_docker.sh†L45-L156】
- **Execution mode & DB**: `RUN_HTTPD` defaults to `true` (`case.get('RUN_HTTPD', True)`) and decides between the single-container runner and the compose stack; the resolved `RDBMS` drives the compose profiles and entrypoint config merging.【F:tools/test/run_tests.py†L238-L307】【F:etc/docker/dev/rucio/entrypoint.sh†L17-L63】
- **Container runtime & parallelism**: `USE_PODMAN`, `PARALLEL_AUTOTESTS`, `PARALLEL_AUTOTESTS_PROCNUM`, `PARALLEL_AUTOTESTS_FAILFAST`, and `COPY_AUTOTEST_LOGS` control whether cases run in parallel, the worker count, fail-fast behaviour, and optional log collection.【F:tools/test/run_tests.py†L230-L307】
- **Syntax reporting**: `SYNTAX_REPORT=1` (set for the `syntax` suite) makes `tools/test/check_syntax.sh` emit a full Ruff report in addition to the GitHub-format summary.【F:etc/docker/test/matrix.yml†L24-L37】【F:tools/test/check_syntax.sh†L13-L35】
- **Pytest behaviour**: `RUN_XDIST` toggles pytest-xdist and is automatically disabled for sqlite, MySQL, or Oracle runs to avoid locking issues.【F:tools/pytest.sh†L20-L76】
---

## Appendix — Complete inventory of CI/test files

**GitHub Actions (workflows)**

| Item | Description |
| --- | --- |
| `.github/workflows/autotest.yml` | Umbrella workflow that runs header linting, Ruff, annotation and Pyright comparisons, calls the reusable runtime-image workflow, builds the matrix with `matrix_parser.py`, and streams every case into `tools/test/run_tests.py`. |
| `.github/workflows/unit_tests.yml` | Runner-only workflow executing `pytest tests/rucio --cov=lib/rucio` across Python 3.9–3.12 and uploading coverage to Codecov. |
| `.github/workflows/integration_tests.yml` | Container-heavy workflow that parses `matrix_integration_tests.yml`, checks out `rucio/containers`, overrides the compose stack, and runs targeted end-to-end pytest suites. |
| `.github/workflows/vo_tests.yml` | Policy-package workflow that relies on `votest_helper.py` to build the VO matrix and then runs each case through `tools/test/run_tests.py`. |
| `.github/workflows/runtime_images.yml` | Reusable workflow that hashes the runtime Dockerfile plus requirements, builds Python 3.9/3.10 runtime images, and pushes tags to GHCR. |
| `.github/workflows/imagecache.yml` | Scheduled cache builder enumerating release branches, expanding matrices, and prebuilding autotest/integration images via `build_images.py` with optional cache-busting. |
| `.github/workflows/cleanup_runtime_images.yml` | Cron and on-demand cleanup workflow that prunes aged GHCR runtime images while preserving build-cache tags. |

**Local GitHub actions**

| Item | Description |
| --- | --- |
| `.github/actions/checkout_ancestor_commit/` | Composite action used by `autotest.yml` to fetch the ancestor of `rucio/master` for annotation and Pyright regressions. |

**GitHub workflow helpers**

| Item | Description |
| --- | --- |
| `tools/github/workflow/grabrelease.py` | Enumerates release branches (plus `master`) for the image cache workflow via the GitHub API. |
| `tools/github/workflow/set_branch_output_env.py` | Determines the effective branch name and writes it to `$GITHUB_OUTPUT` so downstream jobs can adjust behaviour. |
| `tools/github/workflow/util.py` | Shared helper utilities (HTTP wrappers, pagination helpers) imported by the workflow scripts above. |

**External registries & repositories**

| Item | Description |
| --- | --- |
| `ghcr.io` (GitHub Container Registry) | Registry hosting runtime, autotest, and integration images; workflows authenticate, push cache artefacts, and pull runtimes from here. |
| `rucio/containers` repository | Companion repository providing the service Dockerfiles and compose definitions that the integration workflow and cache builder check out. |

**Runtime image build assets**

| Item | Description |
| --- | --- |
| `etc/docker/test/runtime.Dockerfile` | Multi-stage Dockerfile that assembles the CI runtime image consumed by autotests and integration tests. |
| `requirements/requirements.server.txt` | Locked server-side dependency set baked into runtime images and CI jobs. |
| `requirements/requirements.dev.txt` | Developer/test dependency lockfile included in runtime images and runner installs. |

**Development compose & entrypoint**

| Item | Description |
| --- | --- |
| `etc/docker/dev/docker-compose.yml` | Compose stack defining the dev `rucio` service, client sidecar, and optional profiles used during tests. |
| `etc/docker/dev/rucio/entrypoint.sh` | Container entrypoint that merges configuration overlays, prepares Alembic files, and updates trust stores before tests run. |
| `etc/docker/dev/rucio/idpsecrets.json` | Identity-provider secrets mounted into the compose stack for authentication scenarios. |

**Matrix descriptors**

| Item | Description |
| --- | --- |
| `etc/docker/test/matrix.yml` | Primary autotest matrix covering syntax, client, remote DBs, sqlite, and multi-VO suites. |
| `etc/docker/test/matrix_nightly.yml` | Nightly matrix variant enabling broader coverage for scheduled runs. |
| `etc/docker/test/matrix_integration_tests.yml` | Integration-test matrix used by `integration_tests.yml` and the cache builder. |
| `etc/docker/test/matrix_policy_package_tests.yml` | VO-specific matrix enumerating policy packages, databases, and runtimes for `vo_tests.yml`. |

**Config overlays & Alembic (consumed by entrypoint)**

| Item | Description |
| --- | --- |
| `etc/docker/test/extra/rucio_autotests_common.cfg` | Base overlay merged for containerised DB runs prior to adding engine-specific tweaks. |
| `etc/docker/test/extra/rucio_client.cfg` | Client-only configuration applied when the `client` suite executes. |
| `etc/docker/test/extra/rucio_default.cfg` | Default configuration copied verbatim when no `RDBMS` override is requested. |
| `etc/docker/test/extra/rucio_oracle.cfg` | Oracle overlay merged on top of the common config for Oracle autotest cases. |
| `etc/docker/test/extra/rucio_mysql8.cfg` | MySQL 8 overlay merged for MySQL-based autotest suites. |
| `etc/docker/test/extra/rucio_sqlite.cfg` | SQLite overlay merged for single-container sqlite runs. |
| `etc/docker/test/extra/rucio_postgres14.cfg` | PostgreSQL 14 overlay merged for the default remote DB suites. |
| `etc/docker/test/extra/alembic_default.ini` | Baseline Alembic configuration copied when no DB-specific file is needed. |
| `etc/docker/test/extra/alembic_oracle.ini` | Alembic configuration tuned for Oracle migrations during tests. |
| `etc/docker/test/extra/alembic_mysql8.ini` | Alembic configuration tuned for MySQL 8 migrations during tests. |
| `etc/docker/test/extra/alembic_sqlite.ini` | Alembic configuration used for sqlite migration scenarios. |
| `etc/docker/test/extra/alembic_postgres14.ini` | Alembic configuration for PostgreSQL 14 migrations. |
| `etc/docker/test/extra/multi_vo/tst/etc/alembic.ini` | Alembic configuration referenced by multi-VO test overlays. |
| `alembic.ini.template` | Template used to seed Alembic configuration files inside containers. |

**Runners, parsers & helpers**

| Item | Description |
| --- | --- |
| `tools/test/run_tests.py` | Top-level orchestrator that chooses single-container or compose execution paths and dispatches suites. |
| `tools/test/test.sh` | Shell harness invoked inside containers to reset state and launch suite-specific helpers. |
| `tools/test/matrix_parser.py` | YAML-to-JSON matrix converter with allow/deny filtering for autotest and integration matrices. |
| `tools/test/votest_helper.py` | VO matrix validator that prepares per-VO overrides and emits runnable cases. |
| `tools/merge_rucio_configs.py` | Utility that layers configuration fragments based on environment variables; used by the runtime entrypoint and multi-VO bootstrap scripts. |
| `tools/test/build_images.py` | Planner that expands matrices into Docker build commands and pushes cache images to GHCR. |
| `tools/test/donkeyrider.py` | Local developer helper that mirrors `build_images.py` logic for ad-hoc test runs. |
| `tools/run_multi_vo_tests_docker.sh` | Full multi-VO bootstrap script optionally activating RSEs before pytest execution. |
| `tools/pytest.sh` | Wrapper around pytest enabling xdist where safe and normalising source paths. |
| `tools/pytest.ini` | Pytest configuration file controlling default markers and options. |
| `tools/run_tests.sh` | Initialisation helper (used by the client suite) that prepares the environment before invoking pytest. |
| `tools/run_autotests.sh` | Developer convenience script that orchestrates autotest runs outside GitHub Actions. |
| `tools/test/check_syntax.sh` | Ruff-based syntax checker used by the `syntax` and `client_syntax` suites. |
| `tools/test/ignoretool.py` | Utility that builds Ruff include/exclude lists for the client syntax job. |

**DB/bootstrap & test data**

| Item | Description |
| --- | --- |
| `tools/reset_database.py` | Resets and seeds the selected database backend for each suite. |
| `tools/alembic_migration.sh` | Applies Alembic migrations corresponding to the configured database. |
| `tools/bootstrap_tests.py` | Populates baseline accounts, scopes, and configuration for test runs. |
| `tools/sync_rses.py` | Synchronises RSE definitions from `etc/rse_repository.json`. |
| `tools/sync_meta.py` | Aligns metadata schemas used by tests. |
| `tools/docker_activate_rses.sh` | Optional helper that activates RSEs inside dockerised environments. |
| `etc/rse_repository.json` | Canonical repository of RSE definitions consumed during bootstrap. |
| `etc/docker/test/extra/rucio_multi_vo_tst_postgres14.cfg` | Multi-VO test overlay targeting the `tst` VO on PostgreSQL 14. |
| `etc/docker/test/extra/rucio_multi_vo_ts2_postgres14.cfg` | Multi-VO test overlay targeting the `ts2` VO on PostgreSQL 14. |

**Quality & reporting**

| Item | Description |
| --- | --- |
| `tools/add_header` | Script verifying that source files carry the standard Rucio header. |
| `tools/count_missing_type_annotations_utils.sh` | Shared shell helpers for computing missing-annotation reports. |
| `tools/count_missing_type_annotations.sh` | Wrapper invoked after checking out the ancestor commit to compare annotation counts. |
| `tools/run_pyright.sh` | CLI for generating and comparing Pyright reports in CI. |
| `tools/run_pyright/` | Companion scripts and configuration consumed by `tools/run_pyright.sh`. |
| `tests/rucio/` | Unit-test package executed by `unit_tests.yml` for coverage reporting. |
