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
    .venv/bin/python -m pytest --suite=votest

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

Development
-----------

The idea for containerised development is that you use your host machine to edit the files, and test the changes within the container environment. Any change to files in your clone will be reflected into the containers by using volume mounts.

To see your changes in action the recommended way is to jump twice into the container in parallel. One terminal to follow the output of the Rucio server with a shortcut to tail the logfiles (`logshow`), and one terminal to actually run interactive commands:

From your host, get a separate Terminal 1 (the Rucio "server log show")::

    docker compose --file etc/docker/dev/docker-compose.yml exec rucio /bin/bash
    logshow


Terminal 1 can now be left open, and then from your host go into a new Terminal 2 (the "interactive" terminal)::

    docker compose --file etc/docker/dev/docker-compose.yml exec rucio /bin/bash
    rucio whoami


The command will output in Terminal 2, and at the same time the server debug output will be shown in Terminal 1.

The same `logshow` is also available in the FTS container::

    docker compose --file etc/docker/dev/docker-compose.yml --profile storage exec fts /bin/bash
    logshow


Development tricks
------------------

Server changes
~~~~~~~~~~~~~~

If you edit server-side files, e.g. in `lib/rucio/web`, and your changes are not showing up then it is usually helpful to flush the memcache and force the webserver to restart without having to restart the container. Inside the container execute::

    echo 'flush_all' | nc localhost 11211 && httpd -k graceful


Database access
~~~~~~~~~~~~~~~

The default database is PostgreSQL, and `docker-compose` is configured to open its port to the host machine. Using your favourite SQL navigator, e.g., `DBeaver <https://dbeaver.io/>`_, you can connect to the database using the default access on `localhost:5432` to database name `rucio`, schema name `dev`, with username `rucio` and password `secret`.

Docker is eating my disk space
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can reclaim this with::

    docker system prune -f --volumes


Where do I find the Dockerfile
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This container can be found on Dockerhub as `rucio/rucio-dev`, and the corresponding `Dockerfile <https://github.com/rucio/containers/tree/master/dev>`_ is also available. It provides a Rucio environment which allows you to mount your local code in the containers `bin`, `lib`, and `tools` directory. The container is set up to run against a PostgreSQL database with fsync and most durability features for the WAL disabled to improve testing IO throughput. Tests and checks can be run against the development code without having to rebuild the container.


I need a Docker based on another branch (not rucio/master)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In such case, you can download the Rucio container files and e.g. choose to modify the dev container before build::

    # in a different directory
    git clone https://github.com/rucio/containers
    cd containers/dev


Change anything you need, e.g. in the Dockerfile the code branch cloned to your docker container::

    # from
    RUN git clone https://github.com/rucio/rucio.git /tmp/rucio
    # to e.g.:
    RUN git clone --single-branch --branch next https://github.com/rucio/rucio.git /tmp/rucio
    # build your docker
    docker build -t rucio/rucio-dev .


Compose as usual using docker-compose::

    cd /path/to/your/rucio/clone
    docker compose --file etc/docker/dev/docker-compose.yml up -d



Start the daemons
~~~~~~~~~~~~~~~~~~~

Daemons are not running in the docker environment, but all daemons support single-execution mode with the --run-once argument. Reset the system first with::

    python -m tests.ruciopytest.infra_manager \
        --case integration-py39-postgres14


Some files are created. Let's add them to a new dataset::

    rucio add-dataset test:mynewdataset
    rucio attach test:mynewdataset test:file1 test:file2 test:file3 test:file4


If you run the command below, the files are not in the RSE XRD3, but only in XRD1 and 2.::

    rucio list-file-replicas test:mynewdataset
    > +---------+--------+------------+-----------+------------------------------------------------+
    > | SCOPE   | NAME   | FILESIZE   | ADLER32   | RSE: REPLICA                                   |
    > |---------+--------+------------+-----------+------------------------------------------------|
    > | test    | file1  | 10.486 MB  | 141a641e  | XRD1: root://xrd1:1094//rucio/test/80/25/file1 |
    > | test    | file2  | 10.486 MB  | fdfa7eea  | XRD1: root://xrd1:1094//rucio/test/f3/14/file2 |
    > | test    | file3  | 10.486 MB  | c669167d  | XRD2: root://xrd2:1095//rucio/test/a9/23/file3 |
    > | test    | file4  | 10.486 MB  | 65786e49  | XRD2: root://xrd2:1095//rucio/test/2b/c2/file4 |
    > +---------+--------+------------+-----------+------------------------------------------------+


So let's add a new rule on our new dataset to oblige Rucio to create replicas also on XRD3::

    rucio add-rule test:mynewdataset 1 XRD3
    > 1aadd685d891400dba050ad43e71fea9


Now we can check the status of the rule. We will see there are 4 files in `Replicating` state::

    rucio rule-info 1aadd685d891400dba050ad43e71fea9|grep Locks
    > Locks OK/REPLICATING/STUCK: 0/4/0


Now we can run the daemons. First the rule evaluation daemon (judge-evaluator) will pick up our rule. Then the transfer submitter daemon (conveyor-submitter) will send the newly created transfers requests to the FTS server. After that, the transfer state check daemon (conveyor-poller) will retrieve from FTS the transfer state information. Finally, the transfer sign-off daemon (conveyor-finisher) updates the internal state of the Rucio catalogue to reflect the changes.::

    rucio-judge-evaluator --run-once
    rucio-conveyor-submitter --run-once
    rucio-conveyor-poller --run-once
    rucio-conveyor-finisher --run-once


If we see the state of the rule now, we see the locks are OK::

    rucio rule-info 1aadd685d891400dba050ad43e71fea9|grep Locks
    > Locks OK/REPLICATING/STUCK: 4/0/0


And if we look at the replicas of the dataset, we see the there are replicas of the files also in XRD3::

    rucio list-file-replicas test:mynewdataset
    > +---------+--------+------------+-----------+------------------------------------------------+
    > | SCOPE   | NAME   | FILESIZE   | ADLER32   | RSE: REPLICA                                   |
    > |---------+--------+------------+-----------+------------------------------------------------|
    > | test    | file1  | 10.486 MB  | 141a641e  | XRD3: root://xrd3:1096//rucio/test/80/25/file1 |
    > | test    | file1  | 10.486 MB  | 141a641e  | XRD1: root://xrd1:1094//rucio/test/80/25/file1 |
    > | test    | file2  | 10.486 MB  | fdfa7eea  | XRD3: root://xrd3:1096//rucio/test/f3/14/file2 |
    > | test    | file2  | 10.486 MB  | fdfa7eea  | XRD1: root://xrd1:1094//rucio/test/f3/14/file2 |
    > | test    | file3  | 10.486 MB  | c669167d  | XRD2: root://xrd2:1095//rucio/test/a9/23/file3 |
    > | test    | file3  | 10.486 MB  | c669167d  | XRD3: root://xrd3:1096//rucio/test/a9/23/file3 |
    > | test    | file4  | 10.486 MB  | 65786e49  | XRD2: root://xrd2:1095//rucio/test/2b/c2/file4 |
    > | test    | file4  | 10.486 MB  | 65786e49  | XRD3: root://xrd3:1096//rucio/test/2b/c2/file4 |
    > +---------+--------+------------+-----------+------------------------------------------------+
