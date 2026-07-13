ARG PYTHON=3.9

FROM almalinux:10.2 AS base
    WORKDIR /usr/local/src
    ARG PYTHON
    ENV PYTHON=$PYTHON
    ENV LANG=en_US.UTF-8
    ENV LC_ALL=en_US.UTF-8
    ENV CPLUS_INCLUDE_PATH="/usr/local/include/python${PYTHON}:/usr/include/python${PYTHON}"
    ENV C_INCLUDE_PATH="/usr/local/include/python${PYTHON}:/usr/include/python${PYTHON}"
    ENV PYTHON_VENV="/opt/venv"
    ENV PATH="/rucio_source/bin:${PYTHON_VENV}/bin:${PATH}"
    ENV PYTHON_39_VERSION="3.9.25"
    ENV PYTHON_39_SHA256="a7438eabd3a48139f42d4e058096af8d880b0bb6e8fb8c78838892e4ce5583f2"
    ENV PYTHON_310_VERSION="3.10.20"
    ENV PYTHON_310_SHA256="4ff5fd4c5bab803b935019f3e31d7219cebd6f870d00389cea53b88bbe935d1a"
    ENV PYTHON_311_VERSION="3.11.15"
    ENV PYTHON_311_SHA256="f4de1b10bd6c70cbb9fa1cd71fc5038b832747a74ee59d599c69ce4846defb50"
    ENV PYTHON_312_VERSION="3.12.13"
    ENV PYTHON_312_SHA256="0816c4761c97ecdb3f50a3924de0a93fd78cb63ee8e6c04201ddfaedca500b0b"
    ENV PYTHON_313_VERSION="3.13.14"
    ENV PYTHON_313_SHA256="5ae535a36af0ebca6fca176ecb8197f5db9c1cb8c8f0cd12cdf1787046db1f41"
    ENV BOOST_VERSION="1.81.0"
    ENV BOOST_SOURCE="boost_1_81_0"
    ENV BOOST_SHA256="205666dea9f6a7cfed87c7a6dfbeb52a2c1b9de55712c9c1a87735d7181452b6"
    ENV GFAL2_PYTHON_VERSION="1.12.0"
    ENV GFAL2_PYTHON_COMMIT="5578406faec80fb7520e1dee2cf86dbcc56aad71"
    ENV RUCIO_HOME="/opt/rucio"

FROM base AS oracle-client
    RUN dnf install -y libnsl libaio nodejs npm
    RUN rpm -i https://download.oracle.com/otn_software/linux/instantclient/1912000/oracle-instantclient19.12-basiclite-19.12.0.0.0-1.x86_64.rpm && \
        echo "/usr/lib/oracle/19.12/client64/lib" > /etc/ld.so.conf.d/oracle-instantclient.conf;

FROM base AS python
    RUN case "$PYTHON" in \
            3.9) PYTHON_VERSION="${PYTHON_39_VERSION}"; PYTHON_SHA256="${PYTHON_39_SHA256}" ;; \
            3.10) PYTHON_VERSION="${PYTHON_310_VERSION}"; PYTHON_SHA256="${PYTHON_310_SHA256}" ;; \
            3.11) PYTHON_VERSION="${PYTHON_311_VERSION}"; PYTHON_SHA256="${PYTHON_311_SHA256}" ;; \
            3.12) PYTHON_VERSION="${PYTHON_312_VERSION}"; PYTHON_SHA256="${PYTHON_312_SHA256}" ;; \
            3.13) PYTHON_VERSION="${PYTHON_313_VERSION}"; PYTHON_SHA256="${PYTHON_313_SHA256}" ;; \
            *) echo "Unsupported Python version: ${PYTHON}" >&2; exit 1 ;; \
        esac && \
        dnf install -y 'dnf-command(config-manager)' && \
        dnf config-manager --enable crb && \
        dnf -y update && \
        dnf -y install dnf-plugins-core && \
        dnf -y --nobest builddep python3 && \
        dnf -y install wget yum-utils make gcc openssl-devel bzip2-devel libffi-devel zlib-devel && \
        wget https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz && \
        echo "${PYTHON_SHA256}  Python-${PYTHON_VERSION}.tgz" | sha256sum --check --strict && \
        tar xzf Python-${PYTHON_VERSION}.tgz && \
        cd Python-${PYTHON_VERSION} && \
        ./configure --enable-optimizations --enable-shared --libdir=/usr/local/lib LDFLAGS="-Wl,-rpath /usr/local/lib" && \
        make -j $(nproc) && \
        make altinstall && \
        cd .. && \
        rm -rf Python-${PYTHON_VERSION} Python-${PYTHON_VERSION}.tgz && \
        dnf remove --assumeyes python3-setuptools && \
        python${PYTHON} -m pip --no-cache-dir install --upgrade pip && \
        python${PYTHON} -m pip --no-cache-dir install --upgrade setuptools wheel
    RUN python${PYTHON} -m venv ${PYTHON_VENV}

FROM python AS gfal2
    RUN dnf install -y epel-release.noarch && \
        dnf install -y 'dnf-command(config-manager)' && \
        dnf config-manager --enable crb && \
        dnf -y install gfal2-devel && \
        wget https://archives.boost.io/release/${BOOST_VERSION}/source/${BOOST_SOURCE}.tar.gz && \
        echo "${BOOST_SHA256}  ${BOOST_SOURCE}.tar.gz" | sha256sum --check --strict && \
        tar -xzf ${BOOST_SOURCE}.tar.gz && \
        cd ${BOOST_SOURCE} && \
        ./bootstrap.sh --with-libraries=python --with-python=/usr/local/bin/python${PYTHON} --prefix=/usr --libdir=/usr/local/lib && \
        ./b2 --with-python --libdir=/usr/local/lib --link=shared linkflags="-Wl,-rpath,/usr/local/lib" && \
        cp /usr/local/src/${BOOST_SOURCE}/stage/lib/lib* /usr/lib64/ && \
        dnf install -y git dnf-plugins-core rpm-build tree which cmake make gcc gcc-c++ && \
        git clone --depth 1 --branch v${GFAL2_PYTHON_VERSION} https://github.com/cern-fts/gfal2-python.git && \
        cd gfal2-python && \
        test "$(git rev-parse HEAD)" = "${GFAL2_PYTHON_COMMIT}" && \
        sed -i 's/"3.10"/"3.10" "3.11" "3.12" "3.13"/' cmake/modules/FindPythonEasy.cmake && \
        cd ./packaging && \
        RPMBUILD_SRC_EXTRA_FLAGS="--without docs --without python2" make srpm && \
        dnf -y builddep ./gfal2-python-${GFAL2_PYTHON_VERSION}-1.*.src.rpm && \
        cd ../ && \
        CPLUS_INCLUDE_PATH=$CPLUS_INCLUDE_PATH:/usr/local/src/${BOOST_SOURCE} python3 -m pip --no-cache-dir install . && \
        cd .. && rm -rf gfal2-python && \
        dnf remove -y boost-python3 && \
        cp ${PYTHON_VENV}/lib/python${PYTHON}/site-packages/gfal2.so /usr/lib64/gfal2.so

FROM python AS mod_wsgi
    ARG MOD_WSGI_VERSION="5.0.2"
    ARG MOD_WSGI_SHA256="9a0fdb61405abc300ec6b100c440dd98cf31cb5f97aeef4207390937298cad20"
    RUN dnf install -y httpd-devel && \
        curl -fsSLo mod_wsgi-${MOD_WSGI_VERSION}.tar.gz https://codeload.github.com/GrahamDumpleton/mod_wsgi/tar.gz/refs/tags/${MOD_WSGI_VERSION} && \
        echo "${MOD_WSGI_SHA256}  mod_wsgi-${MOD_WSGI_VERSION}.tar.gz" | sha256sum --check --strict && \
        tar -xzf mod_wsgi-${MOD_WSGI_VERSION}.tar.gz && \
        cd mod_wsgi-${MOD_WSGI_VERSION} && \
        LDFLAGS="-Wl,-rpath,/usr/local/lib" ./configure --with-python=/usr/local/bin/python${PYTHON} --prefix=/usr --libdir=/usr/local/lib && \
        make -j && \
        make install && \
        echo -e '# NOTE:\n# Only one mod_wsgi can be loaded at a time.\n# Don'"'"'t attempt to load if already loaded.\n<IfModule !wsgi_module>\n    LoadModule wsgi_module modules/mod_wsgi.so\n</IfModule>\n' > /etc/httpd/conf.modules.d/05-wsgi-python.conf;

FROM python AS rucio-runtime
    WORKDIR /usr/local/src

    RUN dnf install -y epel-release.noarch && \
        dnf install -y 'dnf-command(config-manager)' && \
        dnf config-manager --enable crb && \
        dnf install -y \
        xmlsec1-devel xmlsec1-openssl-devel pkg-config libtool-ltdl-devel \
        httpd httpd-devel \
        libnsl libaio \
        memcached \
        gfal2-devel gfal2-all python3-gfal2-util python3-gfal2 \
        python-unversioned-command \
        nodejs npm \
        glibc-langpack-en \
        xrootd-client \
        git \
        swig \
        gmp-devel \
        krb5-devel \
        libxml2-devel \
        mariadb-connector-c \
        mod_ssl \
        mod_auth_gssapi \
        multitail \
        nmap-ncat \
        openssh-clients \
        openssl-devel \
        rsync \
        unzip \
        vim \
        voms-clients-java \
        which && \
        rm -f /etc/httpd/conf.d/ssl.conf /etc/httpd/conf.d/autoindex.conf /etc/httpd/conf.d/userdir.conf /etc/httpd/conf.d/welcome.conf

    # Set up directories and permissions for mounting source code
    RUN mkdir -p /opt/rucio/lib /opt/rucio/bin /opt/rucio/tools /opt/rucio/etc /opt/rucio/tests && \
        mkdir -p /var/log/rucio/trace && \
        chmod -R 777 /var/log/rucio && \
        mkdir -p /etc/grid-security

    # Set environment variable for source directory
    ENV RUCIO_SOURCE_DIR="/rucio_source"

FROM rucio-runtime AS requirements
    # Install Python dependencies
    COPY requirements /tmp/requirements
    RUN dnf -y --skip-broken install make gcc krb5-devel xmlsec1-devel xmlsec1-openssl-devel pkg-config libtool-ltdl-devel git && \
        python3 -m pip --no-cache-dir install --upgrade pip && \
        python3 -m pip --no-cache-dir install --upgrade fts3 && \
        python3 -m pip --no-cache-dir install --upgrade setuptools wheel && \
        python3 -m pip --no-cache-dir install --upgrade \
            -r /tmp/requirements/requirements.server.txt \
            -r /tmp/requirements/requirements.dev.txt \
            -r /tmp/requirements/requirements.devenv.txt
    RUN curl https://rclone.org/install.sh | bash

FROM requirements AS configured

    # Set up Apache configuration
    COPY etc/docker/test/extra/httpd.conf /etc/httpd/conf/httpd.conf
    COPY etc/docker/test/extra/rucio.conf /etc/httpd/conf.d/rucio.conf
    COPY etc/docker/test/extra/00-mpm.conf /etc/httpd/conf.modules.d/00-mpm.conf

    # Copy certificates
    COPY etc/certs/hostcert_rucio.pem /etc/grid-security/hostcert.pem
    COPY etc/certs/hostcert_rucio.key.pem /etc/grid-security/hostkey.pem
    COPY etc/certs/rucio_ca.pem /opt/rucio/etc/rucio_ca.pem
    COPY etc/certs/ruciouser.pem /opt/rucio/etc/ruciouser.pem
    COPY etc/certs/ruciouser.key.pem /opt/rucio/etc/ruciouser.key.pem

    # Create certs dir and symlink for compatibility
    RUN mkdir -p /opt/rucio/etc/certs && \
        ln -s /opt/rucio/etc/rucio_ca.pem /opt/rucio/etc/certs/rucio_ca.pem

    RUN chmod 0400 /etc/grid-security/hostkey.pem && \
        chmod 0400 /opt/rucio/etc/ruciouser.key.pem

    # Copy entrypoint script
    COPY etc/docker/dev/rucio/entrypoint.sh /usr/local/bin/entrypoint.sh
    RUN chmod +x /usr/local/bin/entrypoint.sh

    COPY --from=gfal2 /usr/include/gfal2 /usr/include/gfal2
    COPY --from=gfal2 /usr/lib64/* /usr/lib64/
    COPY --from=gfal2 /usr/lib64/libboost_python3* /usr/lib64/
    COPY --from=gfal2 /usr/lib64/gfal2.so /usr/lib64/gfal2.so

    RUN mv /usr/lib64/gfal2.so ${PYTHON_VENV}/lib/python${PYTHON}/site-packages/gfal2.so;

    COPY --from=mod_wsgi /usr/lib64/httpd/modules /usr/lib64/httpd/modules
    COPY --from=mod_wsgi /etc/httpd/conf.modules.d/05-wsgi-python.conf  /etc/httpd/conf.modules.d/05-wsgi-python.conf

    WORKDIR /opt/rucio
    RUN ldconfig

    ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
    CMD ["httpd","-D","FOREGROUND"]

FROM configured AS final

FROM final AS oracle
    COPY --from=oracle-client /usr/share/oracle /usr/share/oracle
    COPY --from=oracle-client /usr/lib/oracle /usr/lib/oracle/
    COPY --from=oracle-client /etc/ld.so.conf.d/oracle-instantclient.conf /etc/ld.so.conf.d/oracle-instantclient.conf
    RUN ldconfig
