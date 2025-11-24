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

""" Add deleted_did_meta table """

import sqlalchemy as sa
from alembic.op import execute
from sqlalchemy.dialects import postgresql as pg

from rucio.db.sqla.constants import DIDType
from rucio.db.sqla.migrate_repo import (
    create_enum_if_absent_block,
    create_index,
    create_primary_key,
    create_table,
    drop_table,
    get_effective_schema,
    is_current_dialect,
    try_drop_enum,
)
from rucio.db.sqla.types import JSON

# Alembic revision identifiers
revision = 'b0070f3695c8'
down_revision = 'b5493606bbf5'


def upgrade():
    """
    Upgrade the database to this revision
    """

    if is_current_dialect('oracle', 'mysql', 'postgresql'):

        # ---- PostgreSQL enum hardening (idempotent) ----
        if is_current_dialect('postgresql'):
            schema = get_effective_schema()
            # Emulate "CREATE TYPE IF NOT EXISTS" safely
            execute(
                create_enum_if_absent_block(
                    'DEL_DID_META_DID_TYPE_CHK',
                    [e.value for e in DIDType],
                )
            )
            did_type_enum = pg.ENUM(
                *[e.value for e in DIDType],
                name='DEL_DID_META_DID_TYPE_CHK',
                schema=schema,
                create_type=False,  # reuse existing type, never create here
            )
        else:
            # Non-PG backends use a CHECK-based Enum as before
            did_type_enum = sa.Enum(
                DIDType,
                name='DEL_DID_META_DID_TYPE_CHK',
                create_constraint=True,
                values_callable=lambda obj: [e.value for e in obj],
            )

        create_table(
            'deleted_did_meta',
            sa.Column('scope', sa.String(25)),
            sa.Column('name', sa.String(255)),
            sa.Column('did_type', did_type_enum),
            sa.Column('meta', JSON()),
            sa.Column('created_at', sa.DateTime),
            sa.Column('updated_at', sa.DateTime),
            sa.Column('deleted_at', sa.DateTime)
        )

        create_primary_key('DEL_DID_META_PK', 'deleted_did_meta', ['scope', 'name'])
        create_index('DEL_DID_META_DID_TYPE_IDX', 'deleted_did_meta', ['did_type'])


def downgrade():
    """
    Downgrade the database to the previous revision
    """

    if is_current_dialect('oracle', 'mysql', 'postgresql'):
        drop_table('deleted_did_meta')

        # On PostgreSQL drop the enum type once the table has been removed.
        if is_current_dialect('postgresql'):
            try_drop_enum('DEL_DID_META_DID_TYPE_CHK')
