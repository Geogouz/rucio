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

""" add didtype_chck to requests """

import sqlalchemy as sa
from alembic.op import execute
from sqlalchemy.dialects import postgresql as pg

from rucio.db.sqla.constants import DIDType
from rucio.db.sqla.migrate_repo import (
    add_column,
    create_enum_if_absent_block,
    drop_column,
    get_effective_schema,
    is_current_dialect,
    try_drop_enum,
)

# Alembic revision identifiers
revision = '1a29d6a9504c'
down_revision = '436827b13f82'


def upgrade():
    """
    Upgrade the database to this revision
    """

    schema = get_effective_schema()

    if is_current_dialect('oracle', 'mysql'):
        add_column('requests', sa.Column('did_type',
                                         sa.Enum(DIDType,
                                                 name='REQUESTS_DIDTYPE_CHK',
                                                 create_constraint=True,
                                                 values_callable=lambda obj: [e.value for e in obj]),
                                         default=DIDType.FILE))
        # we don't want checks on the history table, fake the DID type
        add_column('requests_history', sa.Column('did_type', sa.String(1)))

    elif is_current_dialect('postgresql'):
        enum_values = [did_type.value for did_type in DIDType]
        execute(
            create_enum_if_absent_block(
                'REQUESTS_DIDTYPE_CHK',
                enum_values,
                schema=schema,
            )
        )
        did_type_enum = pg.ENUM(
            *enum_values,
            name='REQUESTS_DIDTYPE_CHK',
            schema=schema,
            create_type=False,
        )
        add_column(
            'requests',
            sa.Column('did_type', did_type_enum, default=DIDType.FILE),
        )
        # we don't want checks on the history table, fake the DID type
        add_column('requests_history', sa.Column('did_type', sa.String(1)))


def downgrade():
    """
    Downgrade the database to the previous revision
    """

    if is_current_dialect('oracle', 'mysql', 'postgresql'):
        drop_column('requests', 'did_type')
        drop_column('requests_history', 'did_type')

    if is_current_dialect('postgresql'):
        try_drop_enum('REQUESTS_DIDTYPE_CHK')
