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

""" Opendata initial model """

import sqlalchemy as sa
from alembic.op import execute, get_bind
from sqlalchemy.dialects import postgresql as pg

from rucio.common.schema import get_schema_value
from rucio.db.sqla.constants import OpenDataDIDState
from rucio.db.sqla.migrate_repo import create_enum_if_absent_block, create_index, create_table, drop_table, get_effective_schema, is_current_dialect, try_drop_enum, try_drop_index
from rucio.db.sqla.types import JSON

# Alembic revision identifiers
revision = 'a62db546a1f1'
down_revision = '30d5206e9cad'


def upgrade():
    schema = get_effective_schema()
    enum_values = [state.value for state in OpenDataDIDState]

    if is_current_dialect('postgresql'):
        execute(
            create_enum_if_absent_block(
                'DID_OPENDATA_STATE_CHK',
                enum_values,
            )
        )
        state_enum = pg.ENUM(
            *enum_values,
            name='DID_OPENDATA_STATE_CHK',
            schema=schema,
            create_type=False,
        )
    else:
        state_enum = sa.Enum(
            OpenDataDIDState,
            name='DID_OPENDATA_STATE_CHK',
            create_constraint=True,
            values_callable=lambda obj: [e.value for e in obj],
        )

    create_table(
        'dids_opendata',
        sa.Column('scope', sa.String(length=get_schema_value('SCOPE_LENGTH')), nullable=False),
        sa.Column('name', sa.String(length=get_schema_value('NAME_LENGTH')), nullable=False),
        sa.Column('state', state_enum, nullable=True,
                  server_default=OpenDataDIDState.DRAFT.value),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('scope', 'name', name='OPENDATA_DID_PK'),
        sa.ForeignKeyConstraint(['scope', 'name'], ['dids.scope', 'dids.name'],
                                ondelete='CASCADE', name='OPENDATA_DID_FK')
    )
    create_index('OPENDATA_DID_UPDATED_AT_IDX', 'dids_opendata', ['updated_at'])
    create_index('OPENDATA_DID_CREATED_AT_IDX', 'dids_opendata', ['created_at'])
    create_index('OPENDATA_DID_STATE_IDX', 'dids_opendata', ['state'])
    create_index('OPENDATA_DID_STATE_UPDATED_AT_IDX', 'dids_opendata', ['state', 'updated_at'])

    create_table(
        'dids_opendata_doi',
        sa.Column('scope', sa.String(length=get_schema_value('SCOPE_LENGTH')), nullable=False),
        sa.Column('name', sa.String(length=get_schema_value('NAME_LENGTH')), nullable=False),
        sa.Column('doi', sa.String(length=255), nullable=False, unique=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('scope', 'name', name='OPENDATA_DOI_PK'),
        sa.ForeignKeyConstraint(['scope', 'name'], ['dids_opendata.scope', 'dids_opendata.name'],
                                ondelete='CASCADE', name='OPENDATA_DOI_FK')
    )
    create_index('OPENDATA_DOI_UPDATED_AT_IDX', 'dids_opendata_doi', ['updated_at'])
    create_index('OPENDATA_DOI_CREATED_AT_IDX', 'dids_opendata_doi', ['created_at'])

    create_table(
        'dids_opendata_meta',
        sa.Column('scope', sa.String(length=get_schema_value('SCOPE_LENGTH')), nullable=False),
        sa.Column('name', sa.String(length=get_schema_value('NAME_LENGTH')), nullable=False),
        sa.Column('meta', JSON(), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('scope', 'name', name='OPENDATA_META_PK'),
        sa.ForeignKeyConstraint(['scope', 'name'], ['dids_opendata.scope', 'dids_opendata.name'],
                                ondelete='CASCADE', name='OPENDATA_META_FK')
    )


def downgrade():
    drop_table('dids_opendata_meta')

    try_drop_index('OPENDATA_DOI_CREATED_AT_IDX', 'dids_opendata_doi')
    try_drop_index('OPENDATA_DOI_UPDATED_AT_IDX', 'dids_opendata_doi')
    drop_table('dids_opendata_doi')

    try_drop_index('OPENDATA_DID_STATE_UPDATED_AT_IDX', 'dids_opendata')
    try_drop_index('OPENDATA_DID_STATE_IDX', 'dids_opendata')
    try_drop_index('OPENDATA_DID_CREATED_AT_IDX', 'dids_opendata')
    try_drop_index('OPENDATA_DID_UPDATED_AT_IDX', 'dids_opendata')
    drop_table('dids_opendata')

    if is_current_dialect('postgresql'):
        try_drop_enum('DID_OPENDATA_STATE_CHK')
    else:
        sa.Enum(name='DID_OPENDATA_STATE_CHK').drop(get_bind(), checkfirst=True)
