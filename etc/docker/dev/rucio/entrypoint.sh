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

CFG_PATH="$RUCIO_SOURCE_DIR"/etc/docker/test/extra/
if [ -z "$RUCIO_HOME" ]; then
    RUCIO_HOME=/opt/rucio
fi

mkdir -p "$RUCIO_HOME/etc"

generate_rucio_cfg(){
  	local override=$1
  	local destination=$2

    python3 $RUCIO_SOURCE_DIR/tools/merge_rucio_configs.py --use-env \
        -s "$CFG_PATH"/rucio_autotests_common.cfg "$override" \
        -d "$destination"
}

if [ -f /tmp/usercert.pem ]; then
    cp /tmp/usercert.pem "$RUCIO_HOME/etc/"
fi
if [ -f /tmp/userkey.pem ]; then
    cp /tmp/userkey.pem "$RUCIO_HOME/etc/"
    chmod og-rwx "$RUCIO_HOME/etc/userkey.pem"
fi

echo "Generating alembic.ini and rucio.cfg"

if [ -z "$RDBMS" ]; then
    cp "$CFG_PATH"/rucio_default.cfg $RUCIO_HOME/etc/rucio.cfg
    cp "$CFG_PATH"/alembic_default.ini $RUCIO_HOME/etc/alembic.ini

elif [ "$RDBMS" == "oracle_skip" ]; then

    dnf install -y libnsl libaio;

    # Install Oracle Instant Client
    if [ ! -d /usr/lib/oracle ]; then
        IC_VERSION=23.9.0.25.07
        echo "Installing Oracle Instant Client version ${IC_VERSION}..."

        # ----------- import Oracle GPG key ------------
        # Taken from https://docs.oracle.com/en/operating-systems/oracle-linux-manager/2.10/admin/OLM210-ADMIN.pdf
        GPG_KEY_ID=8d8b756f
        GPG_KEY_FILE=/tmp/oracle-gpg-key

        echo "[INFO] Checking for existing Oracle GPG key (${GPG_KEY_ID})..."
        if ! rpm -q gpg-pubkey | grep -qi "$GPG_KEY_ID"; then
            echo "[INFO] Oracle GPG key not found. Downloading and importing..."
            curl -sSL https://yum.oracle.com/RPM-GPG-KEY-oracle-ol9 -o "$GPG_KEY_FILE"
            if [ -s "$GPG_KEY_FILE" ]; then
                rpm --import "$GPG_KEY_FILE"
                echo "[INFO] Oracle GPG key imported successfully."
                rm -f "$GPG_KEY_FILE"
            else
                echo "[ERROR] Failed to download Oracle GPG key or file is empty."
                exit 1
            fi
        else
            echo "[INFO] Oracle GPG key already present."
        fi
        # ----------------------------------------------

        # --------------- build SERIES -----------------
        echo "[INFO] Building version series from IC_VERSION=${IC_VERSION}"
        IFS=. read -r MAJOR UPDATE _ <<<"$IC_VERSION"             # keep only first two fields
        SERIES=$(printf '%-7s' "${MAJOR}${UPDATE}" | tr ' ' 0)    # right‑pad with 0 → 7 digits
        echo "[INFO] Using SERIES=${SERIES}"
        # ----------------------------------------------

        RPM_FILE="oracle-instantclient-basiclite-${IC_VERSION}-1.el9.x86_64.rpm"
        RPM_URL="https://download.oracle.com/otn_software/linux/instantclient/${SERIES}/${RPM_FILE}"
        rpm -ivh "$RPM_URL"

        echo "[INFO] Configuring dynamic linker path..."
        echo "/usr/lib/oracle/${IC_VERSION%.*}/client64/lib" > /etc/ld.so.conf.d/oracle.conf
        ldconfig

        echo "[INFO] Oracle Instant Client installation completed."

        dnf -y update --nobest && \
        dnf -y --skip-broken install make gcc krb5-devel xmlsec1-devel xmlsec1-openssl-devel pkg-config libtool-ltdl-devel git && \
        python3 -m pip --no-cache-dir install --upgrade pip && \
        python3 -m pip --no-cache-dir install --upgrade setuptools wheel && \
        python3 -m pip --no-cache-dir install --upgrade -r rucio_source/requirements/requirements.server.txt -r rucio_source/requirements/requirements.dev.txt

    fi

    generate_rucio_cfg "$CFG_PATH"/rucio_oracle.cfg $RUCIO_HOME/etc/rucio.cfg
    cp "$CFG_PATH"/alembic_oracle.ini $RUCIO_HOME/etc/alembic.ini

elif [ "$RDBMS" == "oracle" ]; then
    generate_rucio_cfg "$CFG_PATH"/rucio_oracle.cfg $RUCIO_HOME/etc/rucio.cfg
    cp "$CFG_PATH"/alembic_oracle.ini $RUCIO_HOME/etc/alembic.ini

elif [ "$RDBMS" == "mysql8" ]; then
    generate_rucio_cfg "$CFG_PATH"/rucio_mysql8.cfg $RUCIO_HOME/etc/rucio.cfg
    cp "$CFG_PATH"/alembic_mysql8.ini $RUCIO_HOME/etc/alembic.ini

elif [ "$RDBMS" == "sqlite" ]; then
    generate_rucio_cfg "$CFG_PATH"/rucio_sqlite.cfg $RUCIO_HOME/etc/rucio.cfg
    cp "$CFG_PATH"/alembic_sqlite.ini $RUCIO_HOME/etc/alembic.ini

elif [ "$RDBMS" == "postgres14" ]; then
    generate_rucio_cfg "$CFG_PATH"/rucio_postgres14.cfg $RUCIO_HOME/etc/rucio.cfg
    cp "$CFG_PATH"/alembic_postgres14.ini $RUCIO_HOME/etc/alembic.ini

fi

update-ca-trust

exec "$@"
