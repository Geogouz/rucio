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

''' create collection replica table '''

import datetime

import sqlalchemy as sa
from alembic import op
from alembic.op import (
    create_check_constraint,
    create_foreign_key,
    create_index,
    create_primary_key,
    create_table,
    drop_table,
)
from sqlalchemy.dialects import postgresql as pg

from rucio.db.sqla.constants import DIDType, ReplicaState
from rucio.db.sqla.migrate_repo import (
    create_enum_if_absent_block,
    drop_enum_sql,
    try_drop_constraint,
)
from rucio.db.sqla.migrate_repo.ddl_helpers import get_effective_schema, is_current_dialect
from rucio.db.sqla.types import GUID

# Alembic revision identifiers
revision = '45378a1e76a8'
down_revision = 'a93e4e47bda'


def upgrade():
    '''
    Upgrade the database to this revision
    '''

    if is_current_dialect('oracle', 'mysql', 'postgresql'):
        schema = get_effective_schema()
        is_pg = is_current_dialect('postgresql')

        type_values = [did_type.value for did_type in DIDType]
        state_values = [state.value for state in ReplicaState]

        if is_pg:
            op.execute(
                create_enum_if_absent_block(
                    'COLLECTION_REPLICAS_TYPE_CHK',
                    type_values,
                    schema=schema,
                )
            )
            op.execute(
                create_enum_if_absent_block(
                    'COLLECTION_REPLICAS_STATE_CHK',
                    state_values,
                    schema=schema,
                )
            )
            did_type_enum = pg.ENUM(
                *type_values,
                name='COLLECTION_REPLICAS_TYPE_CHK',
                schema=schema,
                create_type=False,
            )
            state_enum = pg.ENUM(
                *state_values,
                name='COLLECTION_REPLICAS_STATE_CHK',
                schema=schema,
                create_type=False,
            )
        else:
            did_type_enum = sa.Enum(
                DIDType,
                name='COLLECTION_REPLICAS_TYPE_CHK',
                create_constraint=True,
                values_callable=lambda obj: [e.value for e in obj],
            )
            state_enum = sa.Enum(
                ReplicaState,
                name='COLLECTION_REPLICAS_STATE_CHK',
                create_constraint=True,
                values_callable=lambda obj: [e.value for e in obj],
            )

        create_table('collection_replicas',
                     sa.Column('scope', sa.String(25)),
                     sa.Column('name', sa.String(255)),
                     sa.Column('did_type', did_type_enum),
                     sa.Column('rse_id', GUID()),
                     sa.Column('bytes', sa.BigInteger),
                     sa.Column('length', sa.BigInteger),
                     sa.Column('state', state_enum, default=ReplicaState.UNAVAILABLE),
                     sa.Column('accessed_at', sa.DateTime),
                     sa.Column('created_at', sa.DateTime, default=datetime.datetime.utcnow),
                     sa.Column('updated_at', sa.DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow))

        create_primary_key('COLLECTION_REPLICAS_PK', 'collection_replicas', ['scope', 'name', 'rse_id'])
        create_foreign_key('COLLECTION_REPLICAS_LFN_FK', 'collection_replicas', 'dids', ['scope', 'name'], ['scope', 'name'])
        create_foreign_key('COLLECTION_REPLICAS_RSE_ID_FK', 'collection_replicas', 'rses', ['rse_id'], ['id'])
        create_check_constraint('COLLECTION_REPLICAS_SIZE_NN', 'collection_replicas', 'bytes IS NOT NULL')
        create_check_constraint('COLLECTION_REPLICAS_STATE_NN', 'collection_replicas', 'state IS NOT NULL')
        create_index('COLLECTION_REPLICAS_RSE_ID_IDX', 'collection_replicas', ['rse_id'])


def downgrade():
    '''
    Downgrade the database to the previous revision
    '''

    if is_current_dialect('oracle'):
        try_drop_constraint('COLLECTION_REPLICAS_STATE_CHK', 'collection_replicas')
        drop_table('collection_replicas')

    elif is_current_dialect('postgresql'):
        schema = get_effective_schema()
        # Drop table first so there are no remaining dependencies on enum types.
        drop_table('collection_replicas')

        # Then drop the PostgreSQL enum types so that a subsequent upgrade can recreate them cleanly.
        for enum_name in (
                'COLLECTION_REPLICAS_TYPE_CHK',
                'COLLECTION_REPLICAS_STATE_CHK',
        ):
            op.execute(drop_enum_sql(enum_name, schema=schema))

    elif is_current_dialect('mysql'):
        drop_table('collection_replicas')
