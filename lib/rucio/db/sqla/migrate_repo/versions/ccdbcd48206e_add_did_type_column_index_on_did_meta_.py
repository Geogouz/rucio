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

''' Add did_type column + index on did_meta table '''

import sqlalchemy as sa
from alembic.op import add_column, create_index, drop_column, drop_index, execute
from sqlalchemy.dialects import postgresql as pg

from rucio.db.sqla.constants import DIDType
from rucio.db.sqla.migrate_repo import (
    create_enum_if_absent_block,
    drop_enum_sql,
    try_drop_constraint,
)
from rucio.db.sqla.migrate_repo.ddl_helpers import get_current_dialect, get_effective_schema, qualify_table

# Alembic revision identifiers
revision = 'ccdbcd48206e'
down_revision = '52153819589c'


def upgrade():
    '''
    Upgrade the database to this revision
    '''

    schema = get_effective_schema()
    dialect = get_current_dialect()
    did_meta_table = qualify_table('did_meta', schema)

    if dialect in ['oracle', 'mysql']:
        add_column('did_meta',
                   sa.Column('did_type', sa.Enum(DIDType,
                                                 name='DID_META_DID_TYPE_CHK',
                                                 create_constraint=True,
                                                 values_callable=lambda obj: [e.value for e in obj])),
                   schema=schema)
    elif dialect == 'postgresql':
        enum_values = [did_type.value for did_type in DIDType]
        execute(
            create_enum_if_absent_block(
                'DID_META_DID_TYPE_CHK',
                enum_values,
                schema=schema,
            )
        )
        did_type_enum = pg.ENUM(
            *enum_values,
            name='DID_META_DID_TYPE_CHK',
            schema=schema,
            create_type=False,
        )
        add_column('did_meta', sa.Column('did_type', did_type_enum), schema=schema)
    create_index('DID_META_DID_TYPE_IDX', 'did_meta', ['did_type'])


def downgrade():
    '''
    Downgrade the database to the previous revision
    '''

    drop_index('DID_META_DID_TYPE_IDX', 'did_meta')
    schema = get_effective_schema()
    dialect = get_current_dialect()
    did_meta_table = qualify_table('did_meta', schema)

    if dialect == 'oracle':
        try_drop_constraint('DID_META_DID_TYPE_CHK', 'did_meta')
        drop_column('did_meta', 'did_type', schema=schema)

    elif dialect == 'postgresql':
        execute(
            f'ALTER TABLE {did_meta_table} '
            'DROP CONSTRAINT IF EXISTS "DID_META_DID_TYPE_CHK", ALTER COLUMN did_type TYPE CHAR'
        )
        execute(f'ALTER TABLE {did_meta_table} DROP COLUMN did_type')
        execute(drop_enum_sql('DID_META_DID_TYPE_CHK', schema=schema))

    elif dialect == 'mysql':
        drop_column('did_meta', 'did_type', schema=schema)
