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

""" new bad_pfns table and bad_replicas changes """

import datetime

import sqlalchemy as sa
from alembic.op import create_foreign_key, execute
from sqlalchemy.dialects import postgresql as pg

from rucio.db.sqla.constants import BadPFNStatus
from rucio.db.sqla.migrate_repo import (
    add_column,
    create_check_constraint,
    create_enum_if_absent_block,
    create_index,
    create_primary_key,
    create_table,
    drop_column,
    drop_table,
    get_effective_schema,
    is_current_dialect,
    try_drop_constraint,
    try_drop_enum,
    try_drop_index,
    try_drop_primary_key,
)

# Alembic revision identifiers
revision = 'b96a1c7e1cc4'
down_revision = '1f46c5f240ac'


def upgrade():
    """
    Upgrade the database to this revision
    """

    schema = get_effective_schema()
    state_values = [status.value for status in BadPFNStatus]

    if is_current_dialect('oracle', 'postgresql'):
        is_pg = is_current_dialect('postgresql')
        if is_pg:
            execute(
                create_enum_if_absent_block(
                    'BAD_PFNS_STATE_CHK',
                    state_values,
                )
            )
            bad_pfns_state = pg.ENUM(
                *state_values,
                name='BAD_PFNS_STATE_CHK',
                schema=schema,
                create_type=False,
            )
        else:
            bad_pfns_state = sa.Enum(
                BadPFNStatus,
                name='BAD_PFNS_STATE_CHK',
                create_constraint=True,
                values_callable=lambda obj: [e.value for e in obj],
            )

        # Create new bad_pfns table
        create_table('bad_pfns',
                     sa.Column('path', sa.String(2048)),
                     sa.Column('state', bad_pfns_state, default=BadPFNStatus.SUSPICIOUS),
                     sa.Column('reason', sa.String(255)),
                     sa.Column('account', sa.String(25)),
                     sa.Column('expires_at', sa.DateTime),
                     sa.Column('created_at', sa.DateTime, default=datetime.datetime.utcnow),
                     sa.Column('updated_at', sa.DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow))

        create_primary_key('BAD_PFNS_PK', 'bad_pfns', ['path', 'state'])
        create_foreign_key('BAD_PFNS_ACCOUNT_FK', 'bad_pfns', 'accounts', ['account'], ['account'])

        try_drop_constraint('BAD_REPLICAS_STATE_CHK', 'bad_replicas')
        create_check_constraint(constraint_name='BAD_REPLICAS_STATE_CHK', table_name='bad_replicas',
                                condition="state in ('B', 'D', 'L', 'R', 'S', 'T')")

        # Add new column to bad_replicas table
        add_column('bad_replicas', sa.Column('expires_at', sa.DateTime()))

        # Change PK
        try_drop_primary_key('bad_replicas')
        for pk_name in ('BAD_REPLICAS_STATE_PK', 'BAD_REPLICAS_PK', 'bad_replicas_pkey'):
            try_drop_constraint(pk_name, 'bad_replicas')
        create_primary_key('BAD_REPLICAS_STATE_PK', 'bad_replicas', ['scope', 'name', 'rse_id', 'state', 'created_at'])

        # Add new Index to Table
        create_index('BAD_REPLICAS_EXPIRES_AT_IDX', 'bad_replicas', ['expires_at'])

    elif is_current_dialect('mysql'):
        bad_pfns_state = sa.Enum(
            BadPFNStatus,
            name='BAD_PFNS_STATE_CHK',
            create_constraint=True,
            values_callable=lambda obj: [e.value for e in obj],
        )

        # Create new bad_pfns table
        create_table('bad_pfns',
                     sa.Column('path', sa.String(2048)),
                     sa.Column('state', bad_pfns_state, default=BadPFNStatus.SUSPICIOUS),
                     sa.Column('reason', sa.String(255)),
                     sa.Column('account', sa.String(25)),
                     sa.Column('expires_at', sa.DateTime),
                     sa.Column('created_at', sa.DateTime, default=datetime.datetime.utcnow),
                     sa.Column('updated_at', sa.DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow))

        create_primary_key('BAD_PFNS_PK', 'bad_pfns', ['path', 'state'])
        create_foreign_key('BAD_PFNS_ACCOUNT_FK', 'bad_pfns', 'accounts', ['account'], ['account'])

        try_drop_constraint('BAD_REPLICAS_STATE_CHK', 'bad_replicas')
        create_check_constraint(constraint_name='BAD_REPLICAS_STATE_CHK', table_name='bad_replicas',
                                condition="state in ('B', 'D', 'L', 'R', 'S', 'T')")

        # Add new column to bad_replicas table
        add_column('bad_replicas', sa.Column('expires_at', sa.DateTime()))

        # Change PK
        try_drop_primary_key('bad_replicas')
        create_primary_key('BAD_REPLICAS_STATE_PK', 'bad_replicas', ['scope', 'name', 'rse_id', 'state', 'created_at'])

        # Add new Index to Table
        create_index('BAD_REPLICAS_EXPIRES_AT_IDX', 'bad_replicas', ['expires_at'])


def downgrade():
    """
    Downgrade the database to the previous revision
    """

    if is_current_dialect('oracle'):
        drop_table('bad_pfns')
        try_drop_index('BAD_REPLICAS_EXPIRES_AT_IDX', 'bad_replicas')

        try_drop_constraint('BAD_REPLICAS_STATE_CHK', 'bad_replicas')
        create_check_constraint(constraint_name='BAD_REPLICAS_STATE_CHK', table_name='bad_replicas',
                                condition="state in ('B', 'D', 'L', 'R', 'S')")

        drop_column('bad_replicas', 'expires_at')
        try_drop_primary_key('bad_replicas')
        for pk_name in ('BAD_REPLICAS_STATE_PK', 'BAD_REPLICAS_PK', 'bad_replicas_pkey'):
            try_drop_constraint(pk_name, 'bad_replicas')
        create_primary_key('BAD_REPLICAS_STATE_PK', 'bad_replicas', ['scope', 'name', 'rse_id', 'created_at'])

    elif is_current_dialect('postgresql'):
        drop_table('bad_pfns')

        try_drop_enum('BAD_PFNS_STATE_CHK')

        try_drop_index('BAD_REPLICAS_EXPIRES_AT_IDX', 'bad_replicas')

        try_drop_constraint('BAD_REPLICAS_STATE_CHK', 'bad_replicas')
        alter_column('bad_replicas', 'state', type_=sa.CHAR(length=1))

        create_check_constraint(constraint_name='BAD_REPLICAS_STATE_CHK', table_name='bad_replicas',
                                condition="state in ('B', 'D', 'L', 'R', 'S')")

        drop_column('bad_replicas', 'expires_at')
        try_drop_primary_key('bad_replicas')
        for pk_name in ('BAD_REPLICAS_STATE_PK', 'BAD_REPLICAS_PK', 'bad_replicas_pkey'):
            try_drop_constraint(pk_name, 'bad_replicas')
        create_primary_key('BAD_REPLICAS_STATE_PK', 'bad_replicas', ['scope', 'name', 'rse_id', 'created_at'])

    elif is_current_dialect('mysql'):
        drop_table('bad_pfns')
        try_drop_index('BAD_REPLICAS_EXPIRES_AT_IDX', 'bad_replicas')

        try_drop_constraint('BAD_REPLICAS_STATE_CHK', 'bad_replicas')
        create_check_constraint(constraint_name='BAD_REPLICAS_STATE_CHK', table_name='bad_replicas',
                                condition="state in ('B', 'D', 'L', 'R', 'S')")

        drop_column('bad_replicas', 'expires_at')
        try_drop_primary_key('bad_replicas')
        create_primary_key('BAD_REPLICAS_STATE_PK', 'bad_replicas', ['scope', 'name', 'rse_id', 'created_at'])
