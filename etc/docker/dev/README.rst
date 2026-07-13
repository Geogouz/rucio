Setting up a Rucio development environment
==========================================

Prerequisites
--------------

We provide a containerised version of the Rucio development environment for a quick start. Our containers are ready-made for Docker, but we provide best-effort support for Podman and podman-compose. If you use podman-compose, you need to add `-t cntnet` to the podman-compose commands. For simplicity all commands explained in this documentation are using docker-compose.

To install Docker for your platform, please refer to the `Docker installation guide <https://docs.docker.com/install/>`_, for example, for CentOS `follow these instructions for the Docker Community Edition <https://docs.docker.com/install/linux/docker-ce/centos/>`_. Please make sure that you install this recent Docker version especially if you are on CentOS, i.e. its default version is ancient and does not support some features we rely on.

Start the Docker daemon with `systemctl start docker`. You can confirm that Docker is running properly by executing (might need `sudo`)::

    docker run hello-world

If successful, this will print an informational message telling you that you are ready to go.  Now, also install the `docker-compose` helper tool with `sudo yum install docker-compose` (might need `EPEL <https://fedoraproject.org/wiki/EPEL>`_ enabled). You are now ready to install the Rucio development environment.

Oracle XE cannot run on Apple Silicon through Docker Desktop. Run Oracle cases
with an x86_64 Docker daemon, as described in the
`Oracle XE image documentation <https://github.com/gvenzl/oci-oracle-xe#oracle-xe-on-apple-m-chips>`_.

Preparing the environment
-------------------------

First, fork the `main Rucio repository on GitHub <https://github.com/rucio/rucio>`_ by clicking the yellow Fork Star button, and then clone your private forked Rucio repository to your `~/dev/rucio`. Afterwards add the main upstream repository as an additional remote to be able to submit pull requests later on::

    git clone git@github.com:<your_username>/rucio.git
    cd rucio
    git remote add upstream git@github.com:rucio/rucio.git
    git fetch --all

Now, ensure that the `.git/config` is proper, i.e., mentioning your full name and email address, and create the `.githubtoken` file that contains a full access token from `Github Account Settings <https://github.com/settings/tokens>`_.

Next, start the Rucio development environment with Docker Compose. There are three variants: a standard environment for basic development, one which adds the File Transfer Service (FTS) and storage servers, and one which also adds the monitoring stack.

*Note: if you are running with SELinux enabled, you might run into problems. Check troubleshooting with SELinux on the help pages of your container runtime environment.*

Using the standard environment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Run the containers using Docker Compose (again might need `sudo`)::

    docker compose --file etc/docker/dev/docker-compose.yml up -d

And verify that it is running properly::

    docker compose --file etc/docker/dev/docker-compose.yml ps

This should show you a few running containers: the Rucio server, the PostgreSQL database and the Graphite monitoring.

Finally, you can jump into the container with::

    docker compose --file etc/docker/dev/docker-compose.yml exec rucio /bin/bash

Initialize the catalogue in an existing development container with the same setup implementation used by the test runner::

    docker compose --file etc/docker/dev/docker-compose.yml exec rucio \
        python -m tests.ruciopytest.infra_manager \
        --case remote-dbs-py39-postgres14

Running tests
~~~~~~~~~~~~~

The test runner creates its own isolated Compose project, so a development stack does not need to be running first. Install only its host-side orchestration dependencies::

    python -m pip install --constraint requirements/requirements.dev.txt pytest pyyaml

List the 15 canonical local and CI cases, or run all of them::

    python -m pytest --list-cases
    python -m pytest --suite=all

Run one case, one module, or one test::

    python -m pytest --case=remote-dbs-py39-postgres14
    python -m pytest --case=remote-dbs-py39-postgres14 tests/test_replica.py
    python -m pytest --case=remote-dbs-py39-postgres14 \
        tests/test_replica.py::TestReplicaCore::test_delete_replicas

All normal pytest arguments are forwarded. For example, ``-k``, ``-x``, ``--pdb``, coverage options, and JUnit output work as usual. ``--keep-db`` retains only the selected case's database volume between runs. The supported server test databases are PostgreSQL and Oracle; MySQL and SQLite are not development test targets.

Using the environment including storage
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Again run the containers using Docker Compose::

    docker compose --file etc/docker/dev/docker-compose.yml --profile storage up -d

This should show you a few more running containers: the Rucio server, the PostgreSQL database, FTS and its associated MySQL database, the Graphite monitoring, and three XrootD storage servers.

With this environment you can upload and download data and submit transfers. Initialize the catalogue and storage RSEs with::

    docker compose --file etc/docker/dev/docker-compose.yml --profile storage exec rucio \
        python -m tests.ruciopytest.infra_manager \
        --case integration-py39-postgres14

Run the complete storage and external-metadata integration case in an isolated environment with::

    python -m pytest --case=integration-py39-postgres14

This creates a few random files and uploads them, creates a few datasets and containers, and requests a replication rule for the container, which starts in state REPLICATING. To demonstrate the transfer capability, the daemons can be run in single-execution mode in order:::

    rucio rule-info <rule-id>

    rucio-conveyor-submitter --run-once
    rucio-conveyor-poller --run-once --older-than 0
    rucio-conveyor-finisher --run-once

    rucio rule-info <rule-id>


On the second display of the rule, its state has cleared to OK.

Using the environment including monitoring
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Again run the containers using Docker Compose::

    docker compose --file etc/docker/dev/docker-compose.yml --profile storage --profile monitoring up -d


Now you will have the same containers as before plus a full monitoring stack with Logstash, Elasticsearch, Kibana and Grafana.

To create some events and write them to Elasticsearch first run again the tests as before::

    docker compose --file etc/docker/dev/docker-compose.yml \
        --profile storage --profile monitoring exec rucio \
        python -m tests.ruciopytest.infra_manager \
        --case integration-py39-postgres14


Then you will have to run the transfer daemons (conveyor-\*) and messaging daemon (hermes) to send the events to ActiveMQ. There a script for that which repeats these daemons in single execution mode from the section in a loop::

    run_daemons


When all the daemons ran you will be able to find the events in Kibana. If you run the docker environment on you local machine you can access Kibana at http://localhost:5601. The necessary index pattern will be added automatically. There is also one dashboard available in Kibana. If it is running on remote machine you can SSH forward it::

    ssh -L 5601:127.0.0.1:5601 <hostname>


Additionally, there is also a Grafana server running with one simple dashboard. You can access it at http://localhost:3000. The default credentials are "admin/admin". Also ActiveMQ web console can be accessed at http://localhost:8161.

If you would like to continuously create some transfers and events there are scripts available for that. Open two different shells and in one run::

    create_monit_data


And in the other run::

    run_daemons


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
