Setting up a Rucio development environment
==========================================

Prerequisites
-------------

We provide a containerised version of the Rucio development environment for a
quick start. Docker Compose v2 (``docker compose``) is the supported container
runtime.

Install Docker Engine and the Docker Compose plugin by following the
`Docker installation guide <https://docs.docker.com/engine/install/>`_. Start
the Docker daemon, then verify both Docker and Compose::

    docker run --rm hello-world
    docker compose version

Let Compose select native multi-architecture images. In particular, remove a
host-wide platform override left by another project::

    unset DOCKER_DEFAULT_PLATFORM

Oracle XE cannot run on Apple Silicon through Docker Desktop. Run Oracle cases
with an x86_64 Docker daemon, as described in the
`Oracle XE image documentation <https://github.com/gvenzl/oci-oracle-xe#oracle-xe-on-apple-m-chips>`_.

Preparing the checkout
----------------------

Fork the `Rucio repository <https://github.com/rucio/rucio>`_, clone your fork,
and add the main repository as ``upstream``::

    git clone git@github.com:<your_username>/rucio.git
    cd rucio
    git remote add upstream https://github.com/rucio/rucio.git
    git fetch upstream

Configure your Git name and email before contributing. No GitHub token is
needed to start the development environment.

If SELinux is enabled, consult the Docker documentation if bind mounts are
denied.

Choosing the source version
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The containers bind-mount the current checkout. Stop the development project
before switching branches or tags so that running processes never see a
partially changed tree::

    docker compose --project-name dev \
        --file etc/docker/dev/docker-compose.yml \
        --file etc/docker/dev/docker-compose.ports.yml \
        --profile '*' \
        down --remove-orphans

To use an explicit release, check out its tag and select the matching published
images::

    git fetch upstream --tags
    git switch --detach 37.4.0
    export RUCIO_TAG=37.4.0
    export RUCIO_DEV_PREFIX=release-

This recreates that release's interactive environment. The canonical test
runner documented below is part of the current checkout; use the selected
release's documentation for its version-specific test commands.

To use current ``master``::

    git fetch upstream master
    git switch --detach upstream/master
    unset RUCIO_TAG RUCIO_DEV_PREFIX

To use the current local checkout, make no Git change and use the default
development image::

    unset RUCIO_TAG RUCIO_DEV_PREFIX

Use an explicit release when source and published images must match. This guide
does not infer a Git tag from the mutable registry ``latest`` image because that
mapping is not reproducible.

Starting an interactive development environment
------------------------------------------------

Stop the project before changing its profile set. ``docker compose up`` does
not remove containers from profiles that are no longer selected.

Standard environment
~~~~~~~~~~~~~~~~~~~~

Start the base services and wait for them to become ready::

    docker compose --project-name dev \
        --file etc/docker/dev/docker-compose.yml \
        up --detach --wait

``docker compose up`` pulls images that are missing under the normal Compose
pull policy, so a separate ``docker compose pull`` is not required. To refresh
all images first, or replace a cached image for the wrong architecture, use the
same command with ``--pull always``::

    docker compose --project-name dev \
        --file etc/docker/dev/docker-compose.yml \
        up --detach --wait --pull always

Check the project and open a shell in the Rucio service::

    docker compose --project-name dev \
        --file etc/docker/dev/docker-compose.yml ps
    docker compose --project-name dev \
        --file etc/docker/dev/docker-compose.yml exec rucio /bin/bash

The base environment contains the Rucio server, PostgreSQL, Graphite, InfluxDB,
Elasticsearch, ActiveMQ, and the ``web1`` test endpoint.

Initialize the catalogue in an existing development container::

    docker compose --project-name dev \
        --file etc/docker/dev/docker-compose.yml \
        exec rucio python -m tests.ruciopytest.infra_manager \
        --case remote-dbs-py39-postgres14

Running canonical tests
-----------------------

The test runner creates a separate Compose project for each case. The
interactive ``dev`` project does not need to be running. Create a small host
virtual environment for the orchestration dependencies::

    python3 -m venv .venv
    .venv/bin/python -m pip install \
        --constraint requirements/requirements.dev.txt \
        pytest pytest-cov pytest-xdist pyyaml

List the 15 canonical local and CI cases::

    .venv/bin/python -m pytest --list-cases

Run one case, one module, or one test::

    .venv/bin/python -m pytest --case=remote-dbs-py39-postgres14
    .venv/bin/python -m pytest --case=remote-dbs-py39-postgres14 \
        tests/test_replica.py
    .venv/bin/python -m pytest --case=remote-dbs-py39-postgres14 \
        tests/test_replica.py::TestReplicaCore::test_delete_replicas

Run every matrix case for one named suite::

    .venv/bin/python -m pytest --suite=unit
    .venv/bin/python -m pytest --suite=client
    .venv/bin/python -m pytest --suite=remote_dbs
    .venv/bin/python -m pytest --suite=multi_vo
    .venv/bin/python -m pytest --suite=votest
    .venv/bin/python -m pytest --suite=integration

Run the full canonical suite::

    .venv/bin/python -m pytest --suite=all

The complete integration case is::

    .venv/bin/python -m pytest --case=integration-py39-postgres14

It starts the PostgreSQL test database together with the ``storage``,
``externalmetadata``, and ``iam`` profiles. The test-only ``postgres14`` and
``test-dependencies`` profiles are supplied by ``docker-compose.test.yml`` and
are selected automatically; they are not manual development profiles.

Normal pytest arguments such as ``-k``, ``-x``, ``--pdb``, coverage options,
and JUnit output are forwarded. ``--keep-db`` retains the database volume for
each selected case. PostgreSQL and Oracle are the supported server test
databases; MySQL and SQLite are not development test targets.

Environment including storage
-----------------------------

Start the base environment with the ``storage`` profile::

    DEV_PROFILES=storage docker compose --project-name dev \
        --file etc/docker/dev/docker-compose.yml \
        --profile storage \
        up --detach --wait

Storage adds FTS and its internal MySQL database, five XRootD servers, MinIO,
and an SSH server. The FTS database is not a Rucio catalogue test target. This
environment supports uploads, downloads, and transfer submission.

Complete interactive integration environment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For an interactive equivalent of the integration service topology, activate
``storage``, ``externalmetadata``, and ``iam``. The main PostgreSQL database is
already part of the base environment::

    DEV_PROFILES=storage,externalmetadata,iam \
    docker compose --project-name dev \
        --file etc/docker/dev/docker-compose.yml \
        --profile storage \
        --profile externalmetadata \
        --profile iam \
        up --detach --wait

The external-metadata profile adds MongoDB with and without authentication, a
metadata PostgreSQL service, and a metadata Elasticsearch service. The IAM
profile adds its MariaDB service, INDIGO IAM, its login service, and Keycloak.

Initialize the catalogue and integration RSEs::

    docker compose --project-name dev \
        --file etc/docker/dev/docker-compose.yml \
        --profile storage \
        --profile externalmetadata \
        --profile iam \
        exec rucio python -m tests.ruciopytest.infra_manager \
        --case integration-py39-postgres14

Use the canonical pytest command rather than this long-lived environment when
the result must match CI exactly.

Environment including monitoring
--------------------------------

Start storage and monitoring with the ports overlay so the web interfaces are
available on the host::

    DEV_PROFILES=storage,monitoring \
    docker compose --project-name dev \
        --file etc/docker/dev/docker-compose.yml \
        --file etc/docker/dev/docker-compose.ports.yml \
        --profile storage \
        --profile monitoring \
        up --detach --wait

Monitoring adds Kibana and Grafana. Elasticsearch is already a base service,
and the development Hermes configuration sends events to it directly.

Prepare the catalogue and storage RSEs, then enter the Rucio container::

    docker compose --project-name dev \
        --file etc/docker/dev/docker-compose.yml \
        --file etc/docker/dev/docker-compose.ports.yml \
        --profile storage \
        --profile monitoring \
        exec rucio python -m tests.ruciopytest.infra_manager \
        --case integration-py39-postgres14
    docker compose --project-name dev \
        --file etc/docker/dev/docker-compose.yml \
        --file etc/docker/dev/docker-compose.ports.yml \
        --profile storage \
        --profile monitoring \
        exec rucio /bin/bash

Inside the container, process the prepared transfer and publish its events::

    rucio-conveyor-submitter --run-once
    rucio-conveyor-poller --run-once --older-than 0
    rucio-conveyor-finisher --run-once
    rucio-hermes --run-once

Confirm that Hermes populated the development index::

    curl 'http://elasticsearch:9200/rucio-events-dev/_search?pretty'

With the ports overlay, Kibana is available at ``http://localhost:5601`` and
Grafana at ``http://localhost:3000`` (default credentials ``admin/admin``).
Create a Kibana data view for ``rucio-events-*`` using ``created_at`` as its
time field. The ActiveMQ console remains available at
``http://localhost:8161``. Queue-based external monitoring pipelines remain
supported through Hermes' ActiveMQ output; configure that sink explicitly and
follow the `operator monitoring guide
<https://rucio.cern.ch/documentation/operator/monitoring/>`_.

For a remote Docker host, forward a port before opening the corresponding local
URL, for example::

    ssh -L 5601:127.0.0.1:5601 <hostname>

Development workflow
--------------------

Edit files on the host. The checkout is bind-mounted into the containers, so
source changes appear without rebuilding the image. Restart only the affected
process when required.

For example, use one terminal to follow the server logs::

    docker compose --project-name dev \
        --file etc/docker/dev/docker-compose.yml \
        logs --follow rucio

Use another terminal for interactive commands::

    docker compose --project-name dev \
        --file etc/docker/dev/docker-compose.yml exec rucio /bin/bash
    rucio whoami

Follow FTS logs in the same way::

    docker compose --project-name dev \
        --file etc/docker/dev/docker-compose.yml \
        --profile storage logs --follow fts

Server changes
~~~~~~~~~~~~~~

If server-side changes are not visible, flush memcached and gracefully restart
Apache inside the Rucio container::

    echo 'flush_all' | nc localhost 11211 && httpd -k graceful

Database access from the host
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The PostgreSQL port is published only by the ports overlay. Start or update the
base project with that overlay::

    docker compose --project-name dev \
        --file etc/docker/dev/docker-compose.yml \
        --file etc/docker/dev/docker-compose.ports.yml \
        up --detach --wait

After catalogue initialization, connect to ``localhost:5432`` using database
``rucio``, schema ``dev``, user ``rucio``, and password ``secret``.

Stopping and cleaning the project
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Stop and remove only the development project's containers and network. The
wildcard includes any profiles that were started::

    docker compose --project-name dev \
        --file etc/docker/dev/docker-compose.yml \
        --file etc/docker/dev/docker-compose.ports.yml \
        --profile '*' \
        down --remove-orphans

Named volumes are retained by default. Add ``--volumes`` only when a clean
database and service state are required::

    docker compose --project-name dev \
        --file etc/docker/dev/docker-compose.yml \
        --file etc/docker/dev/docker-compose.ports.yml \
        --profile '*' \
        down --remove-orphans --volumes

Container image sources
~~~~~~~~~~~~~~~~~~~~~~~

The published interactive image is ``rucio/rucio-dev``. Its
`Dockerfile <https://github.com/rucio/containers/tree/master/dev>`_ is maintained
in the ``rucio/containers`` repository. This repository owns the Compose service
topology and bind mounts in ``etc/docker/dev``.

Canonical server tests build ``etc/docker/test/runtime.Dockerfile`` from the
current checkout, while unit cases build ``etc/docker/test/unit.Dockerfile``.
Source-only branch changes do not require rebuilding ``rucio-dev`` because the
checkout is mounted into the interactive container.

Running transfer daemons manually
---------------------------------

Daemons are not continuously running in the development environment. After
starting and initializing the complete interactive integration environment,
open a Rucio shell::

    docker compose --project-name dev \
        --file etc/docker/dev/docker-compose.yml \
        --profile storage \
        --profile externalmetadata \
        --profile iam \
        exec rucio /bin/bash

Create a dataset and rule, then run the relevant daemons once::

    rucio add-dataset test:mynewdataset
    rucio attach test:mynewdataset test:file1 test:file2 test:file3 test:file4
    rucio add-rule test:mynewdataset 1 XRD3

    rucio-judge-evaluator --run-once
    rucio-conveyor-submitter --run-once
    rucio-conveyor-poller --run-once --older-than 0
    rucio-conveyor-finisher --run-once

Inspect the rule and replicas with ``rucio rule-info <rule-id>`` and
``rucio list-file-replicas test:mynewdataset``.
