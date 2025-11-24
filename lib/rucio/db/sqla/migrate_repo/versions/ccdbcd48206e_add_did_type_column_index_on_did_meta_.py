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

""" Add did_type column + index on did_meta table """

import sqlalchemy as sa
from alembic.op import execute
from sqlalchemy.dialects import postgresql as pg

from rucio.db.sqla.constants import DIDType
from rucio.db.sqla.migrate_repo import (
    add_column,
    create_enum_if_absent_block,
    create_index,
    drop_column,
    get_effective_schema,
    is_current_dialect,
    qualify_table,
    try_drop_constraint,
    try_drop_enum,
    try_drop_index,
)

# Alembic revision identifiers
revision = 'ccdbcd48206e'
down_revision = '52153819589c'


def upgrade():
    """
    Upgrade the database to this revision
    """

    schema = get_effective_schema()
    did_meta_table = qualify_table('did_meta')
    if is_current_dialect('oracle', 'mysql'):
        add_column('did_meta',
                   sa.Column('did_type', sa.Enum(DIDType,
                                                 name='DID_META_DID_TYPE_CHK',
                                                 create_constraint=True,
                                                 values_callable=lambda obj: [e.value for e in obj])))
    elif is_current_dialect('postgresql'):
        enum_values = [did_type.value for did_type in DIDType]
        execute(
            create_enum_if_absent_block(
                'DID_META_DID_TYPE_CHK',
                enum_values,
            )
        )
        did_type_enum = pg.ENUM(
            *enum_values,
            name='DID_META_DID_TYPE_CHK',
            schema=schema,
            create_type=False,
        )
        add_column('did_meta', sa.Column('did_type', did_type_enum))
    create_index('DID_META_DID_TYPE_IDX', 'did_meta', ['did_type'])


def downgrade():
    """
    Downgrade the database to the previous revision
    """

    did_meta_table = qualify_table('did_meta')
    try_drop_index('DID_META_DID_TYPE_IDX', 'did_meta')
    if is_current_dialect('oracle'):
        try_drop_constraint('DID_META_DID_TYPE_CHK', 'did_meta')
        drop_column('did_meta', 'did_type')

    elif is_current_dialect('postgresql'):
        try_drop_constraint('DID_META_DID_TYPE_CHK', 'did_meta')
        alter_column('did_meta', 'did_type', type_=sa.CHAR(length=1))
        execute(
            f"""
            ALTER TABLE {did_meta_table}
            DROP COLUMN did_type
            """
        )
        try_drop_enum('DID_META_DID_TYPE_CHK')

    elif is_current_dialect('mysql'):
        drop_column('did_meta', 'did_type')
