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

""" new table for lifetime model exceptions """

import datetime

import sqlalchemy as sa
from alembic.op import execute
from sqlalchemy.dialects import postgresql as pg

from rucio.db.sqla.constants import DIDType, LifetimeExceptionsState
from rucio.db.sqla.migrate_repo import (
    create_check_constraint,
    create_enum_if_absent_block,
    create_primary_key,
    create_table,
    drop_table,
    get_effective_schema,
    is_current_dialect,
    try_drop_enum,
)
from rucio.db.sqla.types import GUID

# Alembic revision identifiers
revision = '914b8f02df38'
down_revision = 'fe8ea2fa9788'


def upgrade():
    """
    Upgrade the database to this revision
    """

    if is_current_dialect('oracle', 'mysql', 'postgresql'):
        schema = get_effective_schema()
        is_pg = is_current_dialect('postgresql')

        did_type_values = [did_type.value for did_type in DIDType]
        state_values = [state.value for state in LifetimeExceptionsState]

        if is_pg:
            execute(
                create_enum_if_absent_block(
                    'LIFETIME_EXCEPT_TYPE_CHK',
                    did_type_values,
                    schema=schema,
                )
            )
            execute(
                create_enum_if_absent_block(
                    'LIFETIME_EXCEPT_STATE_CHK',
                    state_values,
                    schema=schema,
                )
            )
            did_type_enum = pg.ENUM(
                *did_type_values,
                name='LIFETIME_EXCEPT_TYPE_CHK',
                schema=schema,
                create_type=False,
            )
            state_enum = pg.ENUM(
                *state_values,
                name='LIFETIME_EXCEPT_STATE_CHK',
                schema=schema,
                create_type=False,
            )
        else:
            did_type_enum = sa.Enum(
                DIDType,
                name='LIFETIME_EXCEPT_TYPE_CHK',
                create_constraint=True,
                values_callable=lambda obj: [e.value for e in obj],
            )
            state_enum = sa.Enum(
                LifetimeExceptionsState,
                name='LIFETIME_EXCEPT_STATE_CHK',
                create_constraint=True,
                values_callable=lambda obj: [e.value for e in obj],
            )

        create_table('lifetime_except',
                     sa.Column('id', GUID()),
                     sa.Column('scope', sa.String(25)),
                     sa.Column('name', sa.String(255)),
                     sa.Column('did_type', did_type_enum),
                     sa.Column('account', sa.String(25)),
                     sa.Column('comments', sa.String(4000)),
                     sa.Column('pattern', sa.String(255)),
                     sa.Column('state', state_enum),
                     sa.Column('created_at', sa.DateTime, default=datetime.datetime.utcnow),
                     sa.Column('updated_at', sa.DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow),
                     sa.Column('expires_at', sa.DateTime))

        create_primary_key('LIFETIME_EXCEPT_PK', 'lifetime_except', ['id', 'scope', 'name', 'did_type', 'account'])
        create_check_constraint('LIFETIME_EXCEPT_SCOPE_NN', 'lifetime_except', 'scope is not null')
        create_check_constraint('LIFETIME_EXCEPT_NAME_NN', 'lifetime_except', 'name is not null')
        create_check_constraint('LIFETIME_EXCEPT_DID_TYPE_NN', 'lifetime_except', 'did_type is not null')


def downgrade():
    """
    Downgrade the database to the previous revision
    """

    # Handle PostgreSQL separately to drop enum types after dropping the table.
    if is_current_dialect('postgresql'):
        drop_table('lifetime_except')
        # Drop enums so a subsequent upgrade can recreate them cleanly.
        try_drop_enum('LIFETIME_EXCEPT_TYPE_CHK')
        try_drop_enum('LIFETIME_EXCEPT_STATE_CHK')

    # Other dialects: just drop the table.
    elif is_current_dialect('oracle', 'mysql'):
        drop_table('lifetime_except')
